// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded columnar MeasurementSet write planning and execution.

use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use casa_tables::{
    ColumnBinding, ColumnOverrides, STREAMING_SCALAR_COLUMN_BUFFER_BYTES,
    STREAMING_TILED_COLUMN_BUFFER_BYTES, StreamedScalarType, StreamedTiledPrimitiveColumn,
    StreamedTiledPrimitiveType, StreamedTiledShapeComplex32Column, StreamedTiledShapeCubeLayout,
    StreamedTiledShapeValueType, StreamingScalarColumnWriter, StreamingTiledPrimitiveWriter,
    StreamingTiledShapeComplex32Writer, StreamingTiledShapeWriter, Table, TableOptions,
    install_streamed_tiled_column, install_streamed_tiled_column_primitive_column,
    install_streamed_tiled_shape_column, install_streamed_tiled_shape_complex32_column,
    install_streamed_tiled_shape_primitive_column,
};
use casa_types::{ArrayValue, PrimitiveType, ScalarValue};
use num_complex::Complex32;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{MeasurementSet, MsError, MsResult, MsSelectionIoBudget};

pub(crate) const INCOMPLETE_WRITE_MARKER: &str = ".casa-rs-write-incomplete";

pub(crate) fn incomplete_write_marker(path: &Path) -> PathBuf {
    path.join(INCOMPLETE_WRITE_MARKER)
}

pub(crate) fn begin_in_place_write(path: &Path) -> MsResult<Option<PathBuf>> {
    if !path.exists() {
        return Ok(None);
    }
    let marker = incomplete_write_marker(path);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
        .map_err(|error| {
            MsError::InvalidInput(format!(
                "cannot begin MeasurementSet write at {}: incomplete marker {}: {error}",
                path.display(),
                marker.display()
            ))
        })?;
    writeln!(file, "pid={}", std::process::id()).map_err(|error| {
        MsError::InvalidInput(format!(
            "cannot record MeasurementSet write marker {}: {error}",
            marker.display()
        ))
    })?;
    file.sync_all().map_err(|error| {
        MsError::InvalidInput(format!(
            "cannot flush MeasurementSet write marker {}: {error}",
            marker.display()
        ))
    })?;
    Ok(Some(marker))
}

pub(crate) fn complete_in_place_write(marker: Option<PathBuf>) -> MsResult<()> {
    if let Some(marker) = marker {
        fs::remove_file(&marker).map_err(|error| {
            MsError::InvalidInput(format!(
                "MeasurementSet data was written but incomplete marker {} could not be removed: {error}",
                marker.display()
            ))
        })?;
    }
    Ok(())
}

/// Resource inputs for a bounded writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetWriteResources {
    /// Bytes available to all writer-owned batches.
    pub available_bytes: usize,
    /// Maximum simultaneously live batches, including the active writer batch.
    pub maximum_live_batches: usize,
    /// I/O buffer bytes reserved by each sequential tiled-column writer.
    pub tiled_column_buffer_bytes: usize,
}

impl MeasurementSetWriteResources {
    /// Derive an explicit writer budget from the same physical-memory policy as readers.
    pub fn from_system_memory(
        maximum_live_batches: usize,
    ) -> Result<Self, MeasurementSetWriteError> {
        let budget = MsSelectionIoBudget::from_system_memory(maximum_live_batches, 1, None)
            .map_err(|error| MeasurementSetWriteError::InvalidPlan(error.to_string()))?;
        Ok(Self {
            available_bytes: budget.available_bytes,
            maximum_live_batches,
            tiled_column_buffer_bytes: STREAMING_TILED_COLUMN_BUFFER_BYTES,
        })
    }
}

fn sequential_tiled_writer_resident_bytes(
    cell_shape: &[usize],
    tile_shape: &[usize],
    element_bytes: usize,
    writer_buffer_bytes: usize,
) -> Result<usize, MeasurementSetWriteError> {
    if tile_shape.len() != cell_shape.len() + 1 || tile_shape.contains(&0) {
        return Err(MeasurementSetWriteError::InvalidPlan(format!(
            "tiled writer shape mismatch: cell={cell_shape:?} tile={tile_shape:?}"
        )));
    }
    let non_row_tiles =
        cell_shape
            .iter()
            .zip(tile_shape)
            .try_fold(1usize, |total, (&cell, &tile)| {
                total
                    .checked_mul(cell.div_ceil(tile))
                    .ok_or(MeasurementSetWriteError::ByteOverflow)
            })?;
    let tile_elements = tile_shape.iter().try_fold(1usize, |total, extent| {
        total
            .checked_mul(*extent)
            .ok_or(MeasurementSetWriteError::ByteOverflow)
    })?;
    non_row_tiles
        .checked_mul(tile_elements)
        .and_then(|elements| elements.checked_mul(element_bytes))
        .and_then(|tile_bytes| tile_bytes.checked_add(writer_buffer_bytes))
        .ok_or(MeasurementSetWriteError::ByteOverflow)
}

/// Largest standard visibility cell implied by persisted MS metadata.
///
/// DATA_DESCRIPTION, POLARIZATION, and SPECTRAL_WINDOW are the authoritative
/// shape relationship. This does not inspect MAIN payloads or assume that all
/// rows share one casa-rs-created shape.
#[doc(hidden)]
pub fn maximum_visibility_cell_elements(
    measurement_set: &MeasurementSet,
) -> Result<usize, MeasurementSetWriteError> {
    let data_description = measurement_set
        .data_description()
        .map_err(|error| MeasurementSetWriteError::InvalidPlan(error.to_string()))?;
    let polarization = measurement_set
        .polarization()
        .map_err(|error| MeasurementSetWriteError::InvalidPlan(error.to_string()))?;
    let spectral_window = measurement_set
        .spectral_window()
        .map_err(|error| MeasurementSetWriteError::InvalidPlan(error.to_string()))?;
    let mut maximum = 0usize;
    for row in 0..data_description.row_count() {
        let polarization_id = data_description
            .polarization_id(row)
            .map_err(|error| MeasurementSetWriteError::InvalidPlan(error.to_string()))?;
        let spectral_window_id = data_description
            .spectral_window_id(row)
            .map_err(|error| MeasurementSetWriteError::InvalidPlan(error.to_string()))?;
        let polarization_row = usize::try_from(polarization_id).map_err(|_| {
            MeasurementSetWriteError::InvalidPlan(format!(
                "DATA_DESCRIPTION row {row} has negative POLARIZATION_ID {polarization_id}"
            ))
        })?;
        let spectral_window_row = usize::try_from(spectral_window_id).map_err(|_| {
            MeasurementSetWriteError::InvalidPlan(format!(
                "DATA_DESCRIPTION row {row} has negative SPECTRAL_WINDOW_ID {spectral_window_id}"
            ))
        })?;
        let correlations = usize::try_from(
            polarization
                .num_corr(polarization_row)
                .map_err(|error| MeasurementSetWriteError::InvalidPlan(error.to_string()))?,
        )
        .map_err(|_| {
            MeasurementSetWriteError::InvalidPlan(format!(
                "POLARIZATION row {polarization_row} has a negative correlation count"
            ))
        })?;
        let channels = usize::try_from(
            spectral_window
                .num_chan(spectral_window_row)
                .map_err(|error| MeasurementSetWriteError::InvalidPlan(error.to_string()))?,
        )
        .map_err(|_| {
            MeasurementSetWriteError::InvalidPlan(format!(
                "SPECTRAL_WINDOW row {spectral_window_row} has a negative channel count"
            ))
        })?;
        maximum = maximum.max(
            correlations
                .checked_mul(channels)
                .ok_or(MeasurementSetWriteError::ByteOverflow)?,
        );
    }
    Ok(maximum)
}

/// Physical operation performed by a MeasurementSet write plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementSetWriteOperation {
    /// Create and publish a new MeasurementSet.
    Create,
    /// Mutate an explicit set of existing MAIN rows.
    SelectedRowMutation,
}

/// Persistence action for one planned MAIN column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementSetColumnWriteMode {
    /// Replace cells in an existing column.
    Replace,
    /// Persist a column added before physical finalization.
    Create,
}

/// Physical data-manager strategy for one planned MAIN column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementSetColumnStorage {
    /// Preserve the data-manager binding read from the existing table.
    Persisted,
    /// Create a fixed-width scalar column in a standard bucket manager.
    Standard,
    /// Create a fixed-shape array column in one tiled hypercube.
    TiledColumn,
    /// Create a variable-shape array column with one hypercube per shape.
    TiledShape,
}

/// Exact physical plan for one MAIN column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetWriteColumnPlan {
    /// MAIN column name.
    pub name: String,
    /// Exact retained payload bytes for one selected row.
    pub bytes_per_row: usize,
    /// Whether the column already exists on disk or is newly created.
    pub mode: MeasurementSetColumnWriteMode,
    /// Persisted or creation-time data-manager strategy.
    pub storage_manager: MeasurementSetColumnStorage,
    /// Existing storage-planner tile geometry for a newly created tiled column.
    pub tile_shape: Option<Vec<usize>>,
    /// Existing tiled column cloned when creating this column, if any.
    pub create_source_column: Option<String>,
}

/// One fixed-width scalar MAIN column owned by a creation session.
#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct MeasurementSetScalarColumnPlan {
    pub name: String,
    pub value_type: StreamedScalarType,
}

impl MeasurementSetScalarColumnPlan {
    fn bytes_per_row(&self) -> usize {
        1 + match self.value_type {
            StreamedScalarType::Bool => 1,
            StreamedScalarType::Int32 | StreamedScalarType::Float32 => 4,
            StreamedScalarType::Float64 => 8,
        }
    }
}

/// Standard fixed-width scalar columns required by a MeasurementSet MAIN table.
#[doc(hidden)]
pub fn standard_main_scalar_column_plans() -> Vec<MeasurementSetScalarColumnPlan> {
    [
        ("ANTENNA1", StreamedScalarType::Int32),
        ("ANTENNA2", StreamedScalarType::Int32),
        ("ARRAY_ID", StreamedScalarType::Int32),
        ("DATA_DESC_ID", StreamedScalarType::Int32),
        ("EXPOSURE", StreamedScalarType::Float64),
        ("FEED1", StreamedScalarType::Int32),
        ("FEED2", StreamedScalarType::Int32),
        ("FIELD_ID", StreamedScalarType::Int32),
        ("FLAG_ROW", StreamedScalarType::Bool),
        ("INTERVAL", StreamedScalarType::Float64),
        ("OBSERVATION_ID", StreamedScalarType::Int32),
        ("PROCESSOR_ID", StreamedScalarType::Int32),
        ("SCAN_NUMBER", StreamedScalarType::Int32),
        ("STATE_ID", StreamedScalarType::Int32),
        ("TIME", StreamedScalarType::Float64),
        ("TIME_CENTROID", StreamedScalarType::Float64),
    ]
    .into_iter()
    .map(|(name, value_type)| MeasurementSetScalarColumnPlan {
        name: name.to_string(),
        value_type,
    })
    .collect()
}

impl MeasurementSetWriteColumnPlan {
    /// Plan one correlation-by-channel array column with the canonical MS tile geometry.
    pub fn visibility_array(
        name: impl Into<String>,
        bytes_per_row: usize,
        mode: MeasurementSetColumnWriteMode,
        correlation_count: usize,
        channel_count: usize,
        telescope_name: &str,
    ) -> Self {
        Self {
            name: name.into(),
            bytes_per_row,
            mode,
            storage_manager: if mode == MeasurementSetColumnWriteMode::Create {
                MeasurementSetColumnStorage::TiledShape
            } else {
                MeasurementSetColumnStorage::Persisted
            },
            tile_shape: Some(crate::ms::casa_visibility_tile_shape(
                correlation_count,
                channel_count,
                telescope_name,
            )),
            create_source_column: None,
        }
    }
}

/// Immutable schema, layout, and resource plan for columnar MeasurementSet writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasurementSetWritePlan {
    /// Creation or selected-row mutation.
    operation: MeasurementSetWriteOperation,
    /// Expected MAIN-table row count.
    row_count: usize,
    /// Correlations per visibility row.
    correlation_count: usize,
    /// Channels per visibility row.
    channel_count: usize,
    /// Exact writer-owned payload bytes for one row.
    bytes_per_row: usize,
    /// Maximum rows accepted in one queued batch.
    batch_rows: usize,
    /// Exact bytes in a full queued batch.
    batch_bytes: usize,
    /// Bounded synchronous queue capacity.
    queue_capacity: usize,
    /// Maximum modeled writer-owned resident bytes.
    maximum_resident_bytes: usize,
    /// Visibility tile geometry `[correlation, channel, row]`.
    visibility_tile_shape: Vec<usize>,
    /// Weight/sigma tile geometry `[correlation, row]`.
    weight_tile_shape: Vec<usize>,
    /// UVW tile geometry `[coordinate, row]`.
    uvw_tile_shape: Vec<usize>,
    /// Explicit I/O buffer capacity passed to every sequential tiled writer.
    tiled_column_buffer_bytes: usize,
    /// Explicit selected MAIN rows for mutation plans.
    selected_rows: Vec<usize>,
    /// Exact columns owned by the operation.
    columns: Vec<MeasurementSetWriteColumnPlan>,
    array_columns: Vec<MeasurementSetArrayColumnPlan>,
    array_column_memory_budget_bytes: usize,
    scalar_columns: Vec<MeasurementSetScalarColumnPlan>,
}

impl MeasurementSetWritePlan {
    /// Maximum rows accepted in one producer batch.
    pub const fn batch_rows(&self) -> usize {
        self.batch_rows
    }

    /// Maximum modeled writer-owned resident bytes.
    pub const fn maximum_resident_bytes(&self) -> usize {
        self.maximum_resident_bytes
    }

    /// Exact planned MAIN columns and their storage strategies.
    pub fn columns(&self) -> &[MeasurementSetWriteColumnPlan] {
        &self.columns
    }

    /// Plan the standard simulation/import visibility column family.
    pub fn visibility_creation(
        row_count: usize,
        correlation_count: usize,
        channel_count: usize,
        telescope_name: &str,
        resources: MeasurementSetWriteResources,
    ) -> Result<Self, MeasurementSetWriteError> {
        if correlation_count == 0 || channel_count == 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "correlation_count and channel_count must be positive".to_string(),
            ));
        }
        if resources.maximum_live_batches == 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "maximum_live_batches must be positive".to_string(),
            ));
        }
        let sample_count = correlation_count
            .checked_mul(channel_count)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let visibility_tile_shape =
            crate::ms::casa_visibility_tile_shape(correlation_count, channel_count, telescope_name);
        let weight_tile_shape = crate::ms::casa_weight_tile_shape(&visibility_tile_shape);
        let uvw_tile_shape = crate::ms::casa_uvw_tile_shape(&visibility_tile_shape);
        let flag_category_tile_shape = [
            visibility_tile_shape[0],
            visibility_tile_shape[1],
            1,
            visibility_tile_shape[2],
        ];
        let scalar_columns = standard_main_scalar_column_plans();
        let scalar_bytes_per_row = scalar_columns.iter().try_fold(0usize, |total, column| {
            total
                .checked_add(column.bytes_per_row())
                .ok_or(MeasurementSetWriteError::ByteOverflow)
        })?;
        let scalar_buffer_bytes = scalar_columns
            .len()
            .checked_mul(STREAMING_SCALAR_COLUMN_BUFFER_BYTES)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let tiled_writer_bytes = [
            (
                &[correlation_count, channel_count][..],
                &visibility_tile_shape[..],
                8usize,
            ),
            (
                &[correlation_count, channel_count][..],
                &visibility_tile_shape[..],
                1usize,
            ),
            (
                &[0, correlation_count, channel_count][..],
                &flag_category_tile_shape[..],
                1usize,
            ),
            (&[3][..], &uvw_tile_shape[..], 8usize),
            (&[correlation_count][..], &weight_tile_shape[..], 4usize),
            (&[correlation_count][..], &weight_tile_shape[..], 4usize),
        ]
        .into_iter()
        .try_fold(0usize, |total, (cell_shape, tile_shape, element_bytes)| {
            let bytes = sequential_tiled_writer_resident_bytes(
                cell_shape,
                tile_shape,
                element_bytes,
                resources.tiled_column_buffer_bytes,
            )?;
            total
                .checked_add(bytes)
                .ok_or(MeasurementSetWriteError::ByteOverflow)
        })?
        .checked_add(
            correlation_count
                .checked_mul(2 * std::mem::size_of::<f32>())
                .ok_or(MeasurementSetWriteError::ByteOverflow)?,
        )
        .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let fixed_writer_bytes = scalar_buffer_bytes
            .checked_add(tiled_writer_bytes)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        if fixed_writer_bytes > resources.available_bytes {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "fixed scalar and tiled column buffers require {fixed_writer_bytes} bytes; writer budget is {} bytes",
                resources.available_bytes
            )));
        }
        // DATA complex32 + FLAG + one possible writer-owned FLAG copy,
        // UVW f64[3], WEIGHT f32[ncorr], SIGMA f32[ncorr], and fixed-width scalars.
        let bytes_per_row = sample_count
            .checked_mul(8 + 1 + 1)
            .and_then(|bytes| bytes.checked_add(3 * 8))
            .and_then(|bytes| bytes.checked_add(correlation_count.checked_mul(4 + 4)?))
            .and_then(|bytes| bytes.checked_add(scalar_bytes_per_row))
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let streaming_budget = resources.available_bytes - fixed_writer_bytes;
        let per_batch_budget = streaming_budget / resources.maximum_live_batches;
        let mut batch_rows = per_batch_budget / bytes_per_row;
        if row_count > 0 && batch_rows == 0 {
            return Err(MeasurementSetWriteError::InsufficientBudget {
                bytes_per_row,
                per_batch_budget,
            });
        }
        let tile_rows = visibility_tile_shape.get(2).copied().unwrap_or(1).max(1);
        if batch_rows >= tile_rows {
            batch_rows -= batch_rows % tile_rows;
        }
        batch_rows = batch_rows.min(row_count.max(1));
        let batch_bytes = batch_rows
            .checked_mul(bytes_per_row)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let queue_budget = streaming_budget.saturating_sub(batch_bytes);
        let queue_capacity = if batch_bytes == 0 {
            0
        } else {
            (queue_budget / batch_bytes).min(resources.maximum_live_batches.saturating_sub(1))
        };
        let maximum_resident_bytes = batch_bytes
            .checked_mul(queue_capacity.saturating_add(1))
            .and_then(|bytes| bytes.checked_add(fixed_writer_bytes))
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let mut columns = vec![
            MeasurementSetWriteColumnPlan {
                name: "DATA".to_string(),
                bytes_per_row: sample_count * 8,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
                tile_shape: Some(visibility_tile_shape.clone()),
                create_source_column: None,
            },
            MeasurementSetWriteColumnPlan {
                name: "FLAG".to_string(),
                bytes_per_row: sample_count,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
                tile_shape: Some(visibility_tile_shape.clone()),
                create_source_column: None,
            },
            MeasurementSetWriteColumnPlan {
                name: "FLAG_CATEGORY".to_string(),
                bytes_per_row: sample_count,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
                tile_shape: None,
                create_source_column: None,
            },
            MeasurementSetWriteColumnPlan {
                name: "UVW".to_string(),
                bytes_per_row: 3 * 8,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledColumn,
                tile_shape: Some(uvw_tile_shape.clone()),
                create_source_column: None,
            },
            MeasurementSetWriteColumnPlan {
                name: "WEIGHT".to_string(),
                bytes_per_row: correlation_count * 4,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
                tile_shape: Some(weight_tile_shape.clone()),
                create_source_column: None,
            },
            MeasurementSetWriteColumnPlan {
                name: "SIGMA".to_string(),
                bytes_per_row: correlation_count * 4,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
                tile_shape: Some(weight_tile_shape.clone()),
                create_source_column: None,
            },
        ];
        columns.extend(
            scalar_columns
                .iter()
                .map(|column| MeasurementSetWriteColumnPlan {
                    name: column.name.clone(),
                    bytes_per_row: column.bytes_per_row(),
                    mode: MeasurementSetColumnWriteMode::Create,
                    storage_manager: MeasurementSetColumnStorage::Standard,
                    tile_shape: None,
                    create_source_column: None,
                }),
        );
        Ok(Self {
            operation: MeasurementSetWriteOperation::Create,
            row_count,
            correlation_count,
            channel_count,
            bytes_per_row,
            batch_rows,
            batch_bytes,
            queue_capacity,
            maximum_resident_bytes,
            visibility_tile_shape: visibility_tile_shape.clone(),
            weight_tile_shape: weight_tile_shape.clone(),
            uvw_tile_shape: uvw_tile_shape.clone(),
            tiled_column_buffer_bytes: resources.tiled_column_buffer_bytes,
            selected_rows: Vec::new(),
            columns,
            array_columns: Vec::new(),
            array_column_memory_budget_bytes: 0,
            scalar_columns,
        })
    }

    /// Plan a bounded mutation of explicit existing MAIN rows.
    pub fn selected_row_mutation(
        selected_rows: Vec<usize>,
        columns: Vec<MeasurementSetWriteColumnPlan>,
        resources: MeasurementSetWriteResources,
    ) -> Result<Self, MeasurementSetWriteError> {
        if resources.maximum_live_batches == 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "maximum_live_batches must be positive".to_string(),
            ));
        }
        if columns.is_empty() && !selected_rows.is_empty() {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mutation requires at least one column".to_string(),
            ));
        }
        if let Some(column) = columns.iter().find(|column| {
            column.create_source_column.is_some()
                && column.mode != MeasurementSetColumnWriteMode::Create
        }) {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "column {} can clone a source only in create mode",
                column.name
            )));
        }
        if let Some(column) = columns.iter().find(|column| {
            (column.mode == MeasurementSetColumnWriteMode::Replace
                && column.storage_manager != MeasurementSetColumnStorage::Persisted)
                || (column.mode == MeasurementSetColumnWriteMode::Create
                    && column.storage_manager != MeasurementSetColumnStorage::TiledShape)
        }) {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "column {} has storage strategy {:?} for {:?} mode",
                column.name, column.storage_manager, column.mode
            )));
        }
        let mut unique_rows = selected_rows.clone();
        unique_rows.sort_unstable();
        unique_rows.dedup();
        if unique_rows.len() != selected_rows.len() {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mutation contains duplicate rows".to_string(),
            ));
        }
        let bytes_per_row = columns.iter().try_fold(0usize, |total, column| {
            total
                .checked_add(column.bytes_per_row)
                .ok_or(MeasurementSetWriteError::ByteOverflow)
        })?;
        if !selected_rows.is_empty() && bytes_per_row == 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mutation bytes_per_row must be positive".to_string(),
            ));
        }
        let per_batch_budget = resources.available_bytes / resources.maximum_live_batches;
        let mut batch_rows = if bytes_per_row == 0 {
            0
        } else {
            per_batch_budget / bytes_per_row
        };
        if !selected_rows.is_empty() && batch_rows == 0 {
            return Err(MeasurementSetWriteError::InsufficientBudget {
                bytes_per_row,
                per_batch_budget,
            });
        }
        let tile_rows = columns
            .iter()
            .filter_map(|column| column.tile_shape.as_ref()?.last().copied())
            .filter(|rows| *rows > 0)
            .min()
            .unwrap_or(1);
        if batch_rows >= tile_rows {
            batch_rows -= batch_rows % tile_rows;
        }
        batch_rows = batch_rows.min(selected_rows.len().max(1));
        let batch_bytes = batch_rows
            .checked_mul(bytes_per_row)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let queue_capacity = if batch_bytes == 0 {
            0
        } else {
            (resources.available_bytes.saturating_sub(batch_bytes) / batch_bytes)
                .min(resources.maximum_live_batches.saturating_sub(1))
        };
        let maximum_resident_bytes = batch_bytes
            .checked_mul(queue_capacity.saturating_add(1))
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        Ok(Self {
            operation: MeasurementSetWriteOperation::SelectedRowMutation,
            row_count: selected_rows.len(),
            correlation_count: 0,
            channel_count: 0,
            bytes_per_row,
            batch_rows,
            batch_bytes,
            queue_capacity,
            maximum_resident_bytes,
            visibility_tile_shape: Vec::new(),
            weight_tile_shape: Vec::new(),
            uvw_tile_shape: Vec::new(),
            tiled_column_buffer_bytes: 0,
            selected_rows,
            columns,
            array_columns: Vec::new(),
            array_column_memory_budget_bytes: 0,
            scalar_columns: Vec::new(),
        })
    }

    /// Plan canonical variable-shape MAIN columns for a new MeasurementSet.
    #[doc(hidden)]
    pub fn variable_array_creation(
        row_count: usize,
        array_columns: Vec<MeasurementSetArrayColumnPlan>,
        scalar_columns: Vec<MeasurementSetScalarColumnPlan>,
        resources: MeasurementSetWriteResources,
    ) -> Result<Self, MeasurementSetWriteError> {
        if resources.maximum_live_batches == 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "maximum_live_batches must be positive".to_string(),
            ));
        }
        if array_columns.is_empty() && scalar_columns.is_empty() && row_count > 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "creation write requires at least one column".to_string(),
            ));
        }
        let mut names = HashSet::with_capacity(array_columns.len() + scalar_columns.len());
        for column in &array_columns {
            if !names.insert(column.name.as_str()) {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "duplicate variable-array column {}",
                    column.name
                )));
            }
            if !matches!(
                column.storage_manager,
                MeasurementSetColumnStorage::TiledColumn | MeasurementSetColumnStorage::TiledShape
            ) {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "array creation column {} requires a tiled creation manager, found {:?}",
                    column.name, column.storage_manager
                )));
            }
            let planned_rows = column.shapes.iter().try_fold(0usize, |total, shape| {
                total
                    .checked_add(shape.row_count)
                    .ok_or(MeasurementSetWriteError::ByteOverflow)
            })?;
            if planned_rows != row_count {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "column {} shape histogram has {planned_rows} rows; plan requires {row_count}",
                    column.name
                )));
            }
            if column.storage_manager == MeasurementSetColumnStorage::TiledColumn
                && (column.shapes.len() != 1
                    || column.shapes[0].row_count != row_count
                    || row_count == 0)
            {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "TiledColumnStMan column {} requires one defined shape for every row",
                    column.name
                )));
            }
        }
        for column in &scalar_columns {
            if !names.insert(column.name.as_str()) {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "duplicate creation column {}",
                    column.name
                )));
            }
        }
        let array_bytes_per_row = array_columns.iter().try_fold(0usize, |total, column| {
            let element_bytes = match column.value_type {
                StreamedTiledShapeValueType::Bool => 1,
                StreamedTiledShapeValueType::Float32 => 4,
                StreamedTiledShapeValueType::Float64 => 8,
                StreamedTiledShapeValueType::Complex32 => 8,
            };
            let column_bytes = column
                .shapes
                .iter()
                .try_fold(0usize, |maximum, shape| {
                    let elements = shape.cell_shape.iter().try_fold(1usize, |total, extent| {
                        total
                            .checked_mul(*extent)
                            .ok_or(MeasurementSetWriteError::ByteOverflow)
                    })?;
                    Ok::<_, MeasurementSetWriteError>(maximum.max(elements))
                })?
                .checked_mul(element_bytes)
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
            total
                .checked_add(column_bytes)
                .ok_or(MeasurementSetWriteError::ByteOverflow)
        })?;
        let scalar_bytes_per_row = scalar_columns.iter().try_fold(0usize, |total, column| {
            total
                .checked_add(column.bytes_per_row())
                .ok_or(MeasurementSetWriteError::ByteOverflow)
        })?;
        let bytes_per_row = array_bytes_per_row
            .checked_add(scalar_bytes_per_row)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let scalar_buffer_bytes = scalar_columns
            .len()
            .checked_mul(STREAMING_SCALAR_COLUMN_BUFFER_BYTES)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        if scalar_buffer_bytes > resources.available_bytes {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "scalar column buffers require {scalar_buffer_bytes} bytes; writer budget is {} bytes",
                resources.available_bytes
            )));
        }
        let streaming_budget = resources.available_bytes - scalar_buffer_bytes;
        let batch_budget = streaming_budget / resources.maximum_live_batches;
        let mut batch_rows = if bytes_per_row == 0 {
            0
        } else {
            batch_budget / bytes_per_row
        };
        if row_count > 0 && batch_rows == 0 {
            return Err(MeasurementSetWriteError::InsufficientBudget {
                bytes_per_row,
                per_batch_budget: batch_budget,
            });
        }
        batch_rows = batch_rows.min(row_count.max(1));
        let batch_bytes = batch_rows
            .checked_mul(bytes_per_row)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let writer_budget = streaming_budget.saturating_sub(batch_bytes);
        let array_column_memory_budget_bytes = if array_columns.is_empty() {
            0
        } else {
            writer_budget / array_columns.len()
        };
        if row_count > 0 && !array_columns.is_empty() && array_column_memory_budget_bytes == 0 {
            return Err(MeasurementSetWriteError::InsufficientBudget {
                bytes_per_row: 1,
                per_batch_budget: 0,
            });
        }
        let maximum_resident_bytes = array_column_memory_budget_bytes
            .checked_mul(array_columns.len())
            .and_then(|bytes| bytes.checked_add(batch_bytes))
            .and_then(|bytes| bytes.checked_add(scalar_buffer_bytes))
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let mut columns = array_columns
            .iter()
            .map(|column| MeasurementSetWriteColumnPlan {
                name: column.name.clone(),
                bytes_per_row: column
                    .shapes
                    .iter()
                    .map(|shape| shape.cell_shape.iter().product::<usize>())
                    .max()
                    .unwrap_or(0)
                    * match column.value_type {
                        StreamedTiledShapeValueType::Bool => 1,
                        StreamedTiledShapeValueType::Float32 => 4,
                        StreamedTiledShapeValueType::Float64
                        | StreamedTiledShapeValueType::Complex32 => 8,
                    },
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: column.storage_manager,
                tile_shape: column.shapes.first().map(|shape| shape.tile_shape.clone()),
                create_source_column: None,
            })
            .collect::<Vec<_>>();
        columns.extend(
            scalar_columns
                .iter()
                .map(|column| MeasurementSetWriteColumnPlan {
                    name: column.name.clone(),
                    bytes_per_row: column.bytes_per_row(),
                    mode: MeasurementSetColumnWriteMode::Create,
                    storage_manager: MeasurementSetColumnStorage::Standard,
                    tile_shape: None,
                    create_source_column: None,
                }),
        );
        Ok(Self {
            operation: MeasurementSetWriteOperation::Create,
            row_count,
            correlation_count: 0,
            channel_count: 0,
            bytes_per_row,
            batch_rows,
            batch_bytes,
            queue_capacity: 0,
            maximum_resident_bytes,
            visibility_tile_shape: Vec::new(),
            weight_tile_shape: Vec::new(),
            uvw_tile_shape: Vec::new(),
            tiled_column_buffer_bytes: 0,
            selected_rows: Vec::new(),
            columns,
            array_columns,
            array_column_memory_budget_bytes,
            scalar_columns,
        })
    }
}

/// One distinct output shape in a bounded variable-array column plan.
#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct MeasurementSetArrayShapePlan {
    pub cell_shape: Vec<usize>,
    pub row_count: usize,
    pub tile_shape: Vec<usize>,
}

impl MeasurementSetArrayShapePlan {
    /// Plan a correlation-by-channel shape with the normal MS tile policy.
    pub fn visibility(
        correlation_count: usize,
        channel_count: usize,
        row_count: usize,
        telescope_name: &str,
    ) -> Self {
        Self {
            cell_shape: vec![correlation_count, channel_count],
            row_count,
            tile_shape: crate::ms::casa_visibility_tile_shape(
                correlation_count,
                channel_count,
                telescope_name,
            ),
        }
    }

    /// Plan the standard FLAG_CATEGORY shape for one visibility shape.
    pub fn flag_category(
        correlation_count: usize,
        channel_count: usize,
        category_count: usize,
        row_count: usize,
        telescope_name: &str,
    ) -> Self {
        let visibility =
            crate::ms::casa_visibility_tile_shape(correlation_count, channel_count, telescope_name);
        Self {
            cell_shape: vec![correlation_count, channel_count, category_count],
            row_count,
            tile_shape: vec![visibility[0], visibility[1], 1, visibility[2]],
        }
    }

    /// Plan a correlation-vector shape with the normal MS weight tile policy.
    pub fn weight(correlation_count: usize, row_count: usize, telescope_name: &str) -> Self {
        let visibility =
            crate::ms::casa_visibility_tile_shape(correlation_count, 1, telescope_name);
        Self {
            cell_shape: vec![correlation_count],
            row_count,
            tile_shape: crate::ms::casa_weight_tile_shape(&visibility),
        }
    }
}

/// One streamed variable-array MAIN column.
#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct MeasurementSetArrayColumnPlan {
    pub name: String,
    pub value_type: StreamedTiledShapeValueType,
    pub shapes: Vec<MeasurementSetArrayShapePlan>,
    pub storage_manager: MeasurementSetColumnStorage,
}

/// One typed standard-visibility column batch.
#[derive(Debug)]
pub(crate) struct MeasurementSetWriteBatch {
    pub data_rows: Vec<Vec<Complex32>>,
    pub flag_rows: Vec<bool>,
    pub uvw_rows: Vec<[f64; 3]>,
}

/// Typed values for one selected-row mutation column batch.
#[derive(Debug)]
#[doc(hidden)]
pub enum MeasurementSetMutationColumnValues {
    Scalars(Vec<ScalarValue>),
    Arrays(Vec<ArrayValue>),
}

impl MeasurementSetMutationColumnValues {
    fn len(&self) -> usize {
        match self {
            Self::Scalars(values) => values.len(),
            Self::Arrays(values) => values.len(),
        }
    }
}

/// One column in a bounded selected-row mutation batch.
#[derive(Debug)]
#[doc(hidden)]
pub struct MeasurementSetMutationColumnBatch {
    pub name: String,
    pub values: MeasurementSetMutationColumnValues,
}

/// Typed selected-row values accepted by [`MeasurementSetWriteSession`].
#[derive(Debug)]
#[doc(hidden)]
pub struct MeasurementSetMutationBatch {
    pub row_indices: Vec<usize>,
    pub columns: Vec<MeasurementSetMutationColumnBatch>,
}

/// Per-column physical-write telemetry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetColumnWriteTelemetry {
    /// MAIN column.
    pub column: String,
    /// Tile assembly wall seconds.
    pub assemble_seconds: f64,
    /// Physical write wall seconds.
    pub write_seconds: f64,
    /// Physical bytes written.
    pub bytes_written: usize,
}

/// Finalized bounded write telemetry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetWriteTelemetry {
    /// Per-column timings and bytes.
    pub columns: Vec<MeasurementSetColumnWriteTelemetry>,
    /// Total tile assembly wall seconds.
    pub assemble_seconds: f64,
    /// Total physical write wall seconds.
    pub write_seconds: f64,
    /// Total physical bytes written.
    pub bytes_written: usize,
    /// Rows completed by the session.
    pub rows_written: usize,
    /// Maximum session-owned resident bytes modeled by the immutable plan.
    pub maximum_resident_bytes: usize,
    /// Time attributable to producer work between session start and finalization.
    pub producer_seconds: f64,
    /// Time producers spent blocked on the bounded creation queue.
    pub queue_wait_seconds: f64,
    /// Flush, installation, marker removal, and other finalization time.
    pub finalize_seconds: f64,
}

/// Failure from canonical MeasurementSet write planning or execution.
#[derive(Debug, Error)]
pub enum MeasurementSetWriteError {
    /// Plan inputs are inconsistent or missing.
    #[error("invalid MeasurementSet write plan: {0}")]
    InvalidPlan(String),
    /// One row cannot fit the stated batch budget.
    #[error(
        "one write row requires {bytes_per_row} bytes but the per-batch budget is {per_batch_budget} bytes"
    )]
    InsufficientBudget {
        /// Exact writer-owned row bytes.
        bytes_per_row: usize,
        /// Bytes available to one batch.
        per_batch_budget: usize,
    },
    /// Checked byte accounting overflowed.
    #[error("MeasurementSet write byte accounting overflowed")]
    ByteOverflow,
    /// A column writer failed.
    #[error("MeasurementSet column write failed: {0}")]
    Column(String),
    /// The background writer stopped or panicked.
    #[error("MeasurementSet background writer failed: {0}")]
    Background(String),
    /// Installing a completed streamed column failed.
    #[error("install MeasurementSet column {column}: {reason}")]
    Install {
        /// MAIN column.
        column: String,
        /// Storage error.
        reason: String,
    },
    /// Preparing or publishing a new output failed.
    #[error("MeasurementSet output {path}: {reason}")]
    Output {
        /// Final output path.
        path: String,
        /// Filesystem failure.
        reason: String,
    },
}

/// Commit-last target for a newly created MeasurementSet.
///
/// The staging directory is not a snapshot or recovery generation. It is the
/// sole new output, renamed into place only after every casacore file is complete.
pub struct MeasurementSetCreateTarget {
    final_path: PathBuf,
    staging_path: PathBuf,
    overwrite: bool,
}

impl MeasurementSetCreateTarget {
    /// Reserve a detectable incomplete staging path for a new output.
    pub fn prepare(
        final_path: impl AsRef<Path>,
        overwrite: bool,
    ) -> Result<Self, MeasurementSetWriteError> {
        let final_path = final_path.as_ref().to_path_buf();
        if final_path.exists() && !overwrite {
            return Err(MeasurementSetWriteError::Output {
                path: final_path.display().to_string(),
                reason: "output already exists".to_string(),
            });
        }
        let file_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| MeasurementSetWriteError::Output {
                path: final_path.display().to_string(),
                reason: "output must have a UTF-8 file name".to_string(),
            })?;
        let staging_path = final_path.with_file_name(format!(
            ".{file_name}.casa-rs-incomplete-{}",
            std::process::id()
        ));
        if staging_path.exists() {
            return Err(MeasurementSetWriteError::Output {
                path: final_path.display().to_string(),
                reason: format!(
                    "incomplete staging output already exists at {}",
                    staging_path.display()
                ),
            });
        }
        Ok(Self {
            final_path,
            staging_path,
            overwrite,
        })
    }

    /// Path to use for all physical creation work.
    pub fn staging_path(&self) -> &Path {
        &self.staging_path
    }

    /// Publish the one completed output with a same-directory rename.
    pub fn commit(self) -> Result<(), MeasurementSetWriteError> {
        if self.final_path.exists() {
            if !self.overwrite {
                return Err(MeasurementSetWriteError::Output {
                    path: self.final_path.display().to_string(),
                    reason: "output appeared before publish".to_string(),
                });
            }
            fs::remove_dir_all(&self.final_path)
                .or_else(|_| fs::remove_file(&self.final_path))
                .map_err(|error| MeasurementSetWriteError::Output {
                    path: self.final_path.display().to_string(),
                    reason: format!("remove existing output before publish: {error}"),
                })?;
        }
        fs::rename(&self.staging_path, &self.final_path).map_err(|error| {
            MeasurementSetWriteError::Output {
                path: self.final_path.display().to_string(),
                reason: format!("publish completed staging output: {error}"),
            }
        })
    }
}

struct StreamedVisibilityColumns {
    data: StreamedTiledShapeComplex32Column,
    flag: StreamedTiledPrimitiveColumn,
    flag_category: StreamedTiledPrimitiveColumn,
    uvw: StreamedTiledPrimitiveColumn,
    weight: StreamedTiledPrimitiveColumn,
    sigma: StreamedTiledPrimitiveColumn,
}

enum MeasurementSetWriteSessionState {
    Creation {
        output: PathBuf,
        sender: mpsc::SyncSender<MeasurementSetWriteBatch>,
        handle: thread::JoinHandle<Result<StreamedVisibilityColumns, MeasurementSetWriteError>>,
        sent_rows: AtomicUsize,
        scalar_writers: HashMap<String, StreamingScalarColumnWriter>,
        queue_wait_nanos: AtomicU64,
        started_at: Instant,
    },
    VariableArrayCreation {
        output: PathBuf,
        writers: HashMap<String, StreamingTiledShapeWriter>,
        scalar_writers: HashMap<String, StreamingScalarColumnWriter>,
        started_at: Instant,
    },
    Mutation {
        incomplete_marker: Option<PathBuf>,
        next_selected_row: usize,
        write_seconds: f64,
        bytes_written: usize,
        column_bytes_written: HashMap<String, usize>,
        started_at: Instant,
    },
}

/// Stateful bounded columnar writer for creation or selected-row mutation.
pub struct MeasurementSetWriteSession {
    plan: MeasurementSetWritePlan,
    state: MeasurementSetWriteSessionState,
}

impl MeasurementSetWriteSession {
    /// Start the physical column writers described by `plan`.
    pub fn start(
        output: impl AsRef<Path>,
        plan: MeasurementSetWritePlan,
    ) -> Result<Self, MeasurementSetWriteError> {
        if plan.operation != MeasurementSetWriteOperation::Create {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "background creation session requires a create plan".to_string(),
            ));
        }
        let output = output.as_ref().to_path_buf();
        if !plan.array_columns.is_empty()
            || (plan.correlation_count == 0 && plan.channel_count == 0)
        {
            let mut writers = HashMap::with_capacity(plan.array_columns.len());
            for column in &plan.array_columns {
                let layouts = column
                    .shapes
                    .iter()
                    .map(|shape| {
                        StreamedTiledShapeCubeLayout::new(
                            shape.cell_shape.clone(),
                            shape.row_count,
                            shape.tile_shape.clone(),
                        )
                    })
                    .collect();
                let writer = StreamingTiledShapeWriter::create(
                    output.join(format!(".casa-rs.{}.table.f.tmp", column.name)),
                    column.value_type,
                    layouts,
                    plan.array_column_memory_budget_bytes,
                    false,
                )
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                writers.insert(column.name.clone(), writer);
            }
            let mut scalar_writers = HashMap::with_capacity(plan.scalar_columns.len());
            for column in &plan.scalar_columns {
                let writer = StreamingScalarColumnWriter::create(
                    output.join(format!(".casa-rs.{}.scalar.tmp", column.name)),
                    plan.row_count,
                    column.value_type,
                )
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                scalar_writers.insert(column.name.clone(), writer);
            }
            return Ok(Self {
                plan,
                state: MeasurementSetWriteSessionState::VariableArrayCreation {
                    output,
                    writers,
                    scalar_writers,
                    started_at: Instant::now(),
                },
            });
        }
        let row_count = plan.row_count;
        let correlation_count = plan.correlation_count;
        let channel_count = plan.channel_count;
        let visibility_tile_shape = plan.visibility_tile_shape.clone();
        let flag_category_tile_shape = vec![
            visibility_tile_shape[0],
            visibility_tile_shape[1],
            1,
            visibility_tile_shape[2],
        ];
        let data_writer = StreamingTiledShapeComplex32Writer::create(
            output.join(".casa-rs.DATA.table.f.tmp"),
            row_count,
            vec![correlation_count, channel_count],
            visibility_tile_shape.clone(),
            plan.tiled_column_buffer_bytes,
            false,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let flag_writer = StreamingTiledPrimitiveWriter::create_shape(
            output.join(".casa-rs.FLAG.table.f.tmp"),
            row_count,
            vec![correlation_count, channel_count],
            visibility_tile_shape,
            StreamedTiledPrimitiveType::Bool,
            plan.tiled_column_buffer_bytes,
            false,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let flag_category_writer = StreamingTiledPrimitiveWriter::create_shape(
            output.join(".casa-rs.FLAG_CATEGORY.table.f.tmp"),
            row_count,
            vec![0, correlation_count, channel_count],
            flag_category_tile_shape,
            StreamedTiledPrimitiveType::Bool,
            plan.tiled_column_buffer_bytes,
            false,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let uvw_writer = StreamingTiledPrimitiveWriter::create_column(
            output.join(".casa-rs.UVW.table.f.tmp"),
            row_count,
            vec![3],
            plan.uvw_tile_shape.clone(),
            StreamedTiledPrimitiveType::Float64,
            plan.tiled_column_buffer_bytes,
            false,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let weight_writer = StreamingTiledPrimitiveWriter::create_shape(
            output.join(".casa-rs.WEIGHT.table.f.tmp"),
            row_count,
            vec![correlation_count],
            plan.weight_tile_shape.clone(),
            StreamedTiledPrimitiveType::Float32,
            plan.tiled_column_buffer_bytes,
            false,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let sigma_writer = StreamingTiledPrimitiveWriter::create_shape(
            output.join(".casa-rs.SIGMA.table.f.tmp"),
            row_count,
            vec![correlation_count],
            plan.weight_tile_shape.clone(),
            StreamedTiledPrimitiveType::Float32,
            plan.tiled_column_buffer_bytes,
            false,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let (sender, receiver) = mpsc::sync_channel(plan.queue_capacity);
        let handle = thread::spawn(move || {
            write_visibility_batches(
                receiver,
                data_writer,
                flag_writer,
                flag_category_writer,
                uvw_writer,
                weight_writer,
                sigma_writer,
                correlation_count,
            )
        });
        let mut scalar_writers = HashMap::with_capacity(plan.scalar_columns.len());
        for column in &plan.scalar_columns {
            let writer = StreamingScalarColumnWriter::create(
                output.join(format!(".casa-rs.{}.scalar.tmp", column.name)),
                plan.row_count,
                column.value_type,
            )
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            scalar_writers.insert(column.name.clone(), writer);
        }
        Ok(Self {
            plan,
            state: MeasurementSetWriteSessionState::Creation {
                output,
                sender,
                handle,
                sent_rows: AtomicUsize::new(0),
                scalar_writers,
                queue_wait_nanos: AtomicU64::new(0),
                started_at: Instant::now(),
            },
        })
    }

    /// Append one boolean variable-array cell in output row order.
    #[doc(hidden)]
    pub fn push_bool_row(
        &mut self,
        column: &str,
        cell_shape: &[usize],
        values: &[bool],
    ) -> Result<(), MeasurementSetWriteError> {
        self.variable_array_writer_mut(column)?
            .push_bool_row(cell_shape, values)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))
    }

    /// Append one float32 variable-array cell in output row order.
    #[doc(hidden)]
    pub fn push_f32_row(
        &mut self,
        column: &str,
        cell_shape: &[usize],
        values: &[f32],
    ) -> Result<(), MeasurementSetWriteError> {
        self.variable_array_writer_mut(column)?
            .push_f32_row(cell_shape, values)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))
    }

    /// Append one float64 variable-array cell in output row order.
    #[doc(hidden)]
    pub fn push_f64_row(
        &mut self,
        column: &str,
        cell_shape: &[usize],
        values: &[f64],
    ) -> Result<(), MeasurementSetWriteError> {
        self.variable_array_writer_mut(column)?
            .push_f64_row(cell_shape, values)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))
    }

    /// Append one complex32 variable-array cell in output row order.
    #[doc(hidden)]
    pub fn push_complex32_row(
        &mut self,
        column: &str,
        cell_shape: &[usize],
        values: &[Complex32],
    ) -> Result<(), MeasurementSetWriteError> {
        self.variable_array_writer_mut(column)?
            .push_complex32_row(cell_shape, values)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))
    }

    /// Append one undefined variable-array cell in output row order.
    #[doc(hidden)]
    pub fn push_undefined_row(&mut self, column: &str) -> Result<(), MeasurementSetWriteError> {
        self.variable_array_writer_mut(column)?
            .push_undefined_row()
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))
    }

    /// Append one fixed-width scalar cell in output row order.
    #[doc(hidden)]
    pub fn push_scalar_row(
        &mut self,
        column: &str,
        value: Option<ScalarValue>,
    ) -> Result<(), MeasurementSetWriteError> {
        let scalar_writers = match &mut self.state {
            MeasurementSetWriteSessionState::Creation { scalar_writers, .. }
            | MeasurementSetWriteSessionState::VariableArrayCreation { scalar_writers, .. } => {
                scalar_writers
            }
            MeasurementSetWriteSessionState::Mutation { .. } => {
                return Err(MeasurementSetWriteError::InvalidPlan(
                    "scalar cells require a creation session".to_string(),
                ));
            }
        };
        scalar_writers
            .get_mut(column)
            .ok_or_else(|| {
                MeasurementSetWriteError::InvalidPlan(format!(
                    "column {column} is not owned by this scalar creation session"
                ))
            })?
            .push(value)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))
    }

    /// Accept a typed batch, splitting it at the immutable plan boundary.
    pub(crate) fn send_batch(
        &self,
        batch: MeasurementSetWriteBatch,
    ) -> Result<(), MeasurementSetWriteError> {
        let MeasurementSetWriteSessionState::Creation {
            sender,
            sent_rows,
            queue_wait_nanos,
            ..
        } = &self.state
        else {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "visibility batches require a creation session".to_string(),
            ));
        };
        if batch.data_rows.len() != batch.flag_rows.len()
            || batch.data_rows.len() != batch.uvw_rows.len()
        {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "batch sizes differ: DATA={} FLAG={} UVW={}",
                batch.data_rows.len(),
                batch.flag_rows.len(),
                batch.uvw_rows.len()
            )));
        }
        let sample_count = self
            .plan
            .correlation_count
            .checked_mul(self.plan.channel_count)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        if let Some((row, actual)) = batch
            .data_rows
            .iter()
            .enumerate()
            .find_map(|(row, data)| (data.len() != sample_count).then_some((row, data.len())))
        {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "DATA row {row} has {actual} samples; plan requires {sample_count}"
            )));
        }
        let batch_rows = batch.data_rows.len();
        if batch_rows > self.plan.batch_rows {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "visibility batch has {batch_rows} rows; plan permits at most {}",
                self.plan.batch_rows
            )));
        }
        sent_rows
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |sent| {
                sent.checked_add(batch_rows)
                    .filter(|total| *total <= self.plan.row_count)
            })
            .map_err(|sent| {
                MeasurementSetWriteError::InvalidPlan(format!(
                    "write would exceed planned row count {} (already sent {sent}, batch {batch_rows})",
                    self.plan.row_count
                ))
            })?;
        let queue_wait_started = Instant::now();
        let result = sender.send(batch);
        queue_wait_nanos.fetch_add(
            duration_nanos_u64(queue_wait_started.elapsed()),
            Ordering::Relaxed,
        );
        result.map_err(|error| MeasurementSetWriteError::Background(error.to_string()))
    }

    /// Finalize and install all columns into an already-saved MAIN descriptor.
    pub fn finish(
        self,
        main: &casa_tables::Table,
    ) -> Result<MeasurementSetWriteTelemetry, MeasurementSetWriteError> {
        let MeasurementSetWriteSession { plan, state } = self;
        match state {
            MeasurementSetWriteSessionState::Creation {
                output,
                sender,
                handle,
                sent_rows,
                scalar_writers,
                queue_wait_nanos,
                started_at,
            } => {
                if !scalar_writers.is_empty() {
                    return Err(MeasurementSetWriteError::InvalidPlan(
                        "scalar creation columns must be finalized with the MAIN descriptor"
                            .to_string(),
                    ));
                }
                let producer_window_seconds = started_at.elapsed().as_secs_f64();
                let finalize_started = Instant::now();
                drop(sender);
                let streamed = handle.join().map_err(|_| {
                    MeasurementSetWriteError::Background("writer thread panicked".to_string())
                })??;
                let actual_rows = sent_rows.load(Ordering::Acquire);
                if actual_rows != plan.row_count {
                    return Err(MeasurementSetWriteError::InvalidPlan(format!(
                        "writer received {actual_rows} rows; plan requires {}",
                        plan.row_count
                    )));
                }
                let telemetry = telemetry(&streamed);
                install_streamed_columns(main, &output, streamed)?;
                Ok(complete_telemetry(
                    telemetry,
                    &plan,
                    producer_window_seconds,
                    queue_wait_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
                    finalize_started.elapsed().as_secs_f64(),
                ))
            }
            MeasurementSetWriteSessionState::VariableArrayCreation {
                output,
                mut writers,
                scalar_writers,
                started_at,
            } => {
                if !scalar_writers.is_empty() {
                    return Err(MeasurementSetWriteError::InvalidPlan(
                        "scalar creation columns must be finalized with the MAIN descriptor"
                            .to_string(),
                    ));
                }
                let producer_window_seconds = started_at.elapsed().as_secs_f64();
                let finalize_started = Instant::now();
                let mut columns = Vec::with_capacity(plan.array_columns.len());
                for column in &plan.array_columns {
                    let writer = writers.remove(&column.name).ok_or_else(|| {
                        MeasurementSetWriteError::InvalidPlan(format!(
                            "missing variable-array writer for {}",
                            column.name
                        ))
                    })?;
                    let streamed = writer
                        .finish()
                        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                    let sequence =
                        crate::ms::measurement_set_main_data_manager_sequence(main, &column.name)
                            .ok_or_else(|| MeasurementSetWriteError::Install {
                            column: column.name.clone(),
                            reason: "data-manager sequence is absent".to_string(),
                        })?;
                    let column_telemetry = column_telemetry(
                        &column.name,
                        streamed.assemble_seconds(),
                        streamed.write_seconds(),
                        streamed.bytes_written(),
                    );
                    match column.storage_manager {
                        MeasurementSetColumnStorage::TiledColumn => {
                            install_streamed_tiled_column(&output, sequence, &column.name, streamed)
                        }
                        MeasurementSetColumnStorage::TiledShape => {
                            install_streamed_tiled_shape_column(
                                &output,
                                sequence,
                                &column.name,
                                streamed,
                            )
                        }
                        other => Err(casa_tables::StorageError::FormatMismatch(format!(
                            "array creation column {} has unsupported storage manager {other:?}",
                            column.name
                        ))),
                    }
                    .map_err(|error| install_error(&column.name, error))?;
                    columns.push(column_telemetry);
                }
                Ok(complete_telemetry(
                    MeasurementSetWriteTelemetry {
                        assemble_seconds: columns
                            .iter()
                            .map(|column| column.assemble_seconds)
                            .sum(),
                        write_seconds: columns.iter().map(|column| column.write_seconds).sum(),
                        bytes_written: columns.iter().map(|column| column.bytes_written).sum(),
                        columns,
                        rows_written: 0,
                        maximum_resident_bytes: 0,
                        producer_seconds: 0.0,
                        queue_wait_seconds: 0.0,
                        finalize_seconds: 0.0,
                    },
                    &plan,
                    producer_window_seconds,
                    0.0,
                    finalize_started.elapsed().as_secs_f64(),
                ))
            }
            MeasurementSetWriteSessionState::Mutation { .. } => {
                Err(MeasurementSetWriteError::InvalidPlan(
                    "creation finalization requires a creation session".to_string(),
                ))
            }
        }
    }

    /// Save a prepared descriptor and install all session-owned creation columns.
    #[doc(hidden)]
    pub fn save_and_finish(
        mut self,
        measurement_set: &mut MeasurementSet,
        column_overrides: &ColumnOverrides,
    ) -> Result<MeasurementSetWriteTelemetry, MeasurementSetWriteError> {
        let scalar_finalize_started = Instant::now();
        let scalar_columns = self.finish_scalar_columns(column_overrides)?;
        let scalar_finalize_seconds = scalar_finalize_started.elapsed().as_secs_f64();
        let save_started = Instant::now();
        measurement_set
            .save_assuming_valid_with_main_column_overrides(&scalar_columns.overrides)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let save_seconds = save_started.elapsed().as_secs_f64();
        let telemetry = self.finish(measurement_set.main_table())?;
        complete_scalar_finalization(
            telemetry,
            scalar_columns,
            scalar_finalize_seconds,
            save_seconds,
        )
    }

    pub(crate) fn save_table_and_finish(
        mut self,
        main: &Table,
        options: TableOptions,
        bindings: &HashMap<String, ColumnBinding>,
        column_overrides: &ColumnOverrides,
    ) -> Result<MeasurementSetWriteTelemetry, MeasurementSetWriteError> {
        let scalar_finalize_started = Instant::now();
        let scalar_columns = self.finish_scalar_columns(column_overrides)?;
        let scalar_finalize_seconds = scalar_finalize_started.elapsed().as_secs_f64();
        let save_started = Instant::now();
        main.save_with_bindings_and_column_overrides_assuming_valid(
            options,
            bindings,
            &scalar_columns.overrides,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let save_seconds = save_started.elapsed().as_secs_f64();
        let telemetry = self.finish(main)?;
        complete_scalar_finalization(
            telemetry,
            scalar_columns,
            scalar_finalize_seconds,
            save_seconds,
        )
    }

    fn finish_scalar_columns(
        &mut self,
        column_overrides: &ColumnOverrides,
    ) -> Result<ScalarColumnFinalization, MeasurementSetWriteError> {
        let mut overrides = column_overrides.clone();
        let scalar_writers = match &mut self.state {
            MeasurementSetWriteSessionState::Creation { scalar_writers, .. }
            | MeasurementSetWriteSessionState::VariableArrayCreation { scalar_writers, .. } => {
                scalar_writers
            }
            MeasurementSetWriteSessionState::Mutation { .. }
                if self.plan.scalar_columns.is_empty() =>
            {
                return Ok(ScalarColumnFinalization::empty(overrides));
            }
            MeasurementSetWriteSessionState::Mutation { .. } => {
                return Err(MeasurementSetWriteError::InvalidPlan(
                    "scalar columns require a creation session".to_string(),
                ));
            }
        };
        let mut writers = std::mem::take(scalar_writers);
        let mut paths = Vec::with_capacity(self.plan.scalar_columns.len());
        let mut telemetry = Vec::with_capacity(self.plan.scalar_columns.len());
        let mut bytes_written = 0usize;
        for column in &self.plan.scalar_columns {
            if overrides.contains_key(&column.name) {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "scalar creation column {} also has an external override",
                    column.name
                )));
            }
            let streamed = writers.remove(&column.name).ok_or_else(|| {
                MeasurementSetWriteError::InvalidPlan(format!(
                    "missing scalar writer for {}",
                    column.name
                ))
            })?;
            let streamed = streamed
                .finish()
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            let column_bytes = self
                .plan
                .row_count
                .checked_mul(column.bytes_per_row())
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
            bytes_written = bytes_written
                .checked_add(column_bytes)
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
            paths.push(streamed.path().to_path_buf());
            overrides.insert_streamed_scalar(&column.name, streamed);
            telemetry.push(column_telemetry(&column.name, 0.0, 0.0, column_bytes));
        }
        Ok(ScalarColumnFinalization {
            overrides,
            paths,
            telemetry,
            bytes_written,
        })
    }

    fn variable_array_writer_mut(
        &mut self,
        column: &str,
    ) -> Result<&mut StreamingTiledShapeWriter, MeasurementSetWriteError> {
        let MeasurementSetWriteSessionState::VariableArrayCreation { writers, .. } =
            &mut self.state
        else {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "variable-array cells require a variable-array creation session".to_string(),
            ));
        };
        writers.get_mut(column).ok_or_else(|| {
            MeasurementSetWriteError::InvalidPlan(format!(
                "column {column} is not owned by this variable-array session"
            ))
        })
    }

    /// Start a bounded selected-row mutation and its incomplete marker.
    #[doc(hidden)]
    pub fn start_selected_row_mutation(
        measurement_set: &mut MeasurementSet,
        plan: MeasurementSetWritePlan,
    ) -> Result<Self, MeasurementSetWriteError> {
        if plan.operation != MeasurementSetWriteOperation::SelectedRowMutation {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row session requires a mutation plan".to_string(),
            ));
        }
        if plan.selected_rows.len() != plan.row_count {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mapping does not match planned row count".to_string(),
            ));
        }
        if plan
            .selected_rows
            .iter()
            .any(|row| *row >= measurement_set.row_count())
        {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mapping contains a row outside MAIN".to_string(),
            ));
        }
        let output = measurement_set.path().ok_or_else(|| {
            MeasurementSetWriteError::InvalidPlan(
                "selected-row mutation requires a disk-backed MeasurementSet".to_string(),
            )
        })?;
        let incomplete_marker = begin_in_place_write(output)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        for column in &plan.columns {
            if column.mode == MeasurementSetColumnWriteMode::Create {
                let already_persisted = measurement_set
                    .main_table()
                    .data_manager_info()
                    .iter()
                    .any(|manager| manager.columns.iter().any(|name| name == &column.name));
                if !already_persisted {
                    if let Some(source) = &column.create_source_column {
                        measurement_set
                            .main_table_mut()
                            .save_added_tiled_column_clone_in_place_assuming_valid(
                                source,
                                &column.name,
                                &format!("Tiled{}", column.name),
                            )
                    } else {
                        measurement_set
                            .main_table_mut()
                            .save_added_tiled_shape_column_in_place_assuming_valid(
                                &column.name,
                                &[],
                                column.tile_shape.as_deref(),
                            )
                    }
                    .map_err(|error| MeasurementSetWriteError::Install {
                        column: column.name.clone(),
                        reason: error.to_string(),
                    })?;
                }
            }
        }
        Ok(Self {
            plan,
            state: MeasurementSetWriteSessionState::Mutation {
                incomplete_marker,
                next_selected_row: 0,
                write_seconds: 0.0,
                bytes_written: 0,
                column_bytes_written: HashMap::new(),
                started_at: Instant::now(),
            },
        })
    }

    /// Persist one typed mutation batch and release it from the table cache.
    #[doc(hidden)]
    pub fn write_mutation_batch(
        &mut self,
        measurement_set: &mut MeasurementSet,
        batch: MeasurementSetMutationBatch,
    ) -> Result<(), MeasurementSetWriteError> {
        let MeasurementSetWriteSessionState::Mutation {
            next_selected_row,
            write_seconds,
            bytes_written,
            column_bytes_written,
            ..
        } = &mut self.state
        else {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "mutation batches require a selected-row session".to_string(),
            ));
        };
        if batch.row_indices.is_empty() {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "mutation batch must contain at least one row".to_string(),
            ));
        }
        if batch.row_indices.len() > self.plan.batch_rows {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "mutation batch has {} rows; plan permits {}",
                batch.row_indices.len(),
                self.plan.batch_rows
            )));
        }
        let end = next_selected_row
            .checked_add(batch.row_indices.len())
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        if self.plan.selected_rows.get(*next_selected_row..end) != Some(&batch.row_indices) {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "mutation batch rows do not match planned rows [{}..{end})",
                *next_selected_row
            )));
        }
        if batch.columns.len() != self.plan.columns.len() {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "mutation batch has {} columns; plan requires {}",
                batch.columns.len(),
                self.plan.columns.len()
            )));
        }

        let plan_columns = self
            .plan
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column))
            .collect::<HashMap<_, _>>();
        let mut seen = HashSet::with_capacity(batch.columns.len());
        let mut batch_bytes = 0usize;
        for column in &batch.columns {
            if !seen.insert(column.name.as_str()) {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "mutation batch repeats column {}",
                    column.name
                )));
            }
            let plan_column = plan_columns.get(column.name.as_str()).ok_or_else(|| {
                MeasurementSetWriteError::InvalidPlan(format!(
                    "mutation batch column {} is absent from the plan",
                    column.name
                ))
            })?;
            if column.values.len() != batch.row_indices.len() {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "mutation column {} has {} values for {} rows",
                    column.name,
                    column.values.len(),
                    batch.row_indices.len()
                )));
            }
            let column_bytes = mutation_values_bytes(&column.values)?;
            let maximum_column_bytes = plan_column
                .bytes_per_row
                .checked_mul(batch.row_indices.len())
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
            if column_bytes > maximum_column_bytes {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "mutation column {} requires {column_bytes} bytes; batch plan permits {maximum_column_bytes}",
                    column.name
                )));
            }
            batch_bytes = batch_bytes
                .checked_add(column_bytes)
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        }
        if batch_bytes > self.plan.batch_bytes {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "mutation batch requires {batch_bytes} bytes; plan permits {}",
                self.plan.batch_bytes
            )));
        }

        let write_started = Instant::now();
        for column in &batch.columns {
            match &column.values {
                MeasurementSetMutationColumnValues::Scalars(values) => {
                    let mut target = measurement_set
                        .main_table_mut()
                        .column_accessor_mut(&column.name)
                        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                    for (&row, value) in batch.row_indices.iter().zip(values) {
                        target
                            .set_scalar_assuming_valid(row, value.clone())
                            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                    }
                }
                MeasurementSetMutationColumnValues::Arrays(values) => {
                    let mut target = measurement_set
                        .main_table_mut()
                        .column_accessor_mut(&column.name)
                        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                    for (&row, value) in batch.row_indices.iter().zip(values) {
                        target
                            .set_array_assuming_valid(row, value.clone())
                            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                    }
                }
            }
        }
        let column_names = batch
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        measurement_set
            .main_table()
            .save_selected_rows_in_place_assuming_valid(&column_names, &batch.row_indices)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        measurement_set
            .main_table_mut()
            .discard_persisted_cell_updates(&column_names, &batch.row_indices);
        *write_seconds += write_started.elapsed().as_secs_f64();
        *bytes_written = bytes_written
            .checked_add(batch_bytes)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        for column in &batch.columns {
            let column_bytes = mutation_values_bytes(&column.values)?;
            let total = column_bytes_written.entry(column.name.clone()).or_default();
            *total = total
                .checked_add(column_bytes)
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        }
        *next_selected_row = end;
        Ok(())
    }

    /// Planned row slice for the next selected-row batch.
    #[doc(hidden)]
    pub fn next_mutation_rows(&self) -> Result<&[usize], MeasurementSetWriteError> {
        let MeasurementSetWriteSessionState::Mutation {
            next_selected_row, ..
        } = &self.state
        else {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "next mutation rows require a selected-row session".to_string(),
            ));
        };
        let end = next_selected_row
            .saturating_add(self.plan.batch_rows)
            .min(self.plan.selected_rows.len());
        Ok(&self.plan.selected_rows[*next_selected_row..end])
    }

    /// Complete a selected-row session after every planned row was written.
    #[doc(hidden)]
    pub fn finish_mutation(self) -> Result<MeasurementSetWriteTelemetry, MeasurementSetWriteError> {
        let MeasurementSetWriteSession { plan, state } = self;
        let MeasurementSetWriteSessionState::Mutation {
            incomplete_marker,
            next_selected_row,
            write_seconds,
            bytes_written,
            column_bytes_written,
            started_at,
        } = state
        else {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "mutation finalization requires a selected-row session".to_string(),
            ));
        };
        if next_selected_row != plan.row_count {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "mutation session wrote {next_selected_row} rows; plan requires {}",
                plan.row_count
            )));
        }
        let producer_window_seconds = started_at.elapsed().as_secs_f64();
        let finalize_started = Instant::now();
        complete_in_place_write(incomplete_marker)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let columns = plan
            .columns
            .iter()
            .map(|column| MeasurementSetColumnWriteTelemetry {
                column: column.name.clone(),
                assemble_seconds: 0.0,
                write_seconds,
                bytes_written: column_bytes_written.get(&column.name).copied().unwrap_or(0),
            })
            .collect();
        Ok(complete_telemetry(
            MeasurementSetWriteTelemetry {
                columns,
                assemble_seconds: 0.0,
                write_seconds,
                bytes_written,
                rows_written: 0,
                maximum_resident_bytes: 0,
                producer_seconds: 0.0,
                queue_wait_seconds: 0.0,
                finalize_seconds: 0.0,
            },
            &plan,
            producer_window_seconds,
            0.0,
            finalize_started.elapsed().as_secs_f64(),
        ))
    }
}

struct ScalarColumnFinalization {
    overrides: ColumnOverrides,
    paths: Vec<PathBuf>,
    telemetry: Vec<MeasurementSetColumnWriteTelemetry>,
    bytes_written: usize,
}

impl ScalarColumnFinalization {
    fn empty(overrides: ColumnOverrides) -> Self {
        Self {
            overrides,
            paths: Vec::new(),
            telemetry: Vec::new(),
            bytes_written: 0,
        }
    }
}

fn complete_scalar_finalization(
    mut telemetry: MeasurementSetWriteTelemetry,
    scalar_columns: ScalarColumnFinalization,
    scalar_finalize_seconds: f64,
    save_seconds: f64,
) -> Result<MeasurementSetWriteTelemetry, MeasurementSetWriteError> {
    telemetry.bytes_written = telemetry
        .bytes_written
        .checked_add(scalar_columns.bytes_written)
        .ok_or(MeasurementSetWriteError::ByteOverflow)?;
    telemetry.columns.extend(scalar_columns.telemetry);
    telemetry.write_seconds += save_seconds;
    telemetry.finalize_seconds += scalar_finalize_seconds + save_seconds;
    telemetry.producer_seconds =
        (telemetry.producer_seconds - scalar_finalize_seconds - save_seconds).max(0.0);
    let cleanup_started = Instant::now();
    for path in scalar_columns.paths {
        fs::remove_file(&path).map_err(|error| {
            MeasurementSetWriteError::Column(format!(
                "remove scalar construction file {}: {error}",
                path.display()
            ))
        })?;
    }
    telemetry.finalize_seconds += cleanup_started.elapsed().as_secs_f64();
    Ok(telemetry)
}

fn primitive_value_bytes(primitive_type: PrimitiveType) -> Option<usize> {
    match primitive_type {
        PrimitiveType::Bool | PrimitiveType::UInt8 => Some(1),
        PrimitiveType::UInt16 | PrimitiveType::Int16 => Some(2),
        PrimitiveType::UInt32 | PrimitiveType::Int32 | PrimitiveType::Float32 => Some(4),
        PrimitiveType::Int64 | PrimitiveType::Float64 | PrimitiveType::Complex32 => Some(8),
        PrimitiveType::Complex64 => Some(16),
        PrimitiveType::String => None,
    }
}

fn mutation_values_bytes(
    values: &MeasurementSetMutationColumnValues,
) -> Result<usize, MeasurementSetWriteError> {
    match values {
        MeasurementSetMutationColumnValues::Scalars(values) => {
            values.iter().try_fold(0usize, |total, value| {
                let bytes = match value {
                    ScalarValue::String(value) => value.len(),
                    other => primitive_value_bytes(other.primitive_type()).ok_or_else(|| {
                        MeasurementSetWriteError::InvalidPlan(
                            "unsupported scalar mutation value".to_string(),
                        )
                    })?,
                };
                total
                    .checked_add(bytes)
                    .ok_or(MeasurementSetWriteError::ByteOverflow)
            })
        }
        MeasurementSetMutationColumnValues::Arrays(values) => {
            values.iter().try_fold(0usize, |total, value| {
                let element_bytes =
                    primitive_value_bytes(value.primitive_type()).ok_or_else(|| {
                        MeasurementSetWriteError::InvalidPlan(
                            "string-array mutation byte accounting is unsupported".to_string(),
                        )
                    })?;
                let bytes = value
                    .len()
                    .checked_mul(element_bytes)
                    .ok_or(MeasurementSetWriteError::ByteOverflow)?;
                total
                    .checked_add(bytes)
                    .ok_or(MeasurementSetWriteError::ByteOverflow)
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn write_visibility_batches(
    receiver: mpsc::Receiver<MeasurementSetWriteBatch>,
    mut data_writer: StreamingTiledShapeComplex32Writer,
    mut flag_writer: StreamingTiledPrimitiveWriter,
    mut flag_category_writer: StreamingTiledPrimitiveWriter,
    mut uvw_writer: StreamingTiledPrimitiveWriter,
    mut weight_writer: StreamingTiledPrimitiveWriter,
    mut sigma_writer: StreamingTiledPrimitiveWriter,
    correlation_count: usize,
) -> Result<StreamedVisibilityColumns, MeasurementSetWriteError> {
    let weight_row = vec![1.0f32; correlation_count];
    let sigma_row = vec![1.0f32; correlation_count];
    for batch in receiver {
        for ((data_row, flag_row), uvw_row) in batch
            .data_rows
            .into_iter()
            .zip(batch.flag_rows)
            .zip(batch.uvw_rows)
        {
            data_writer
                .push_row(&data_row)
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            if flag_row {
                flag_writer
                    .push_bool_fill_row(true)
                    .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            } else {
                flag_writer
                    .push_zero_row()
                    .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            }
            flag_category_writer
                .push_zero_row()
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            uvw_writer
                .push_f64_row(&uvw_row)
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            weight_writer
                .push_f32_row(&weight_row)
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            sigma_writer
                .push_f32_row(&sigma_row)
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        }
    }
    Ok(StreamedVisibilityColumns {
        data: data_writer
            .finish()
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?,
        flag: flag_writer
            .finish()
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?,
        flag_category: flag_category_writer
            .finish()
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?,
        uvw: uvw_writer
            .finish()
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?,
        weight: weight_writer
            .finish()
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?,
        sigma: sigma_writer
            .finish()
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?,
    })
}

fn telemetry(streamed: &StreamedVisibilityColumns) -> MeasurementSetWriteTelemetry {
    let columns = vec![
        column_telemetry(
            "DATA",
            streamed.data.assemble_seconds(),
            streamed.data.write_seconds(),
            streamed.data.bytes_written(),
        ),
        column_telemetry(
            "FLAG",
            streamed.flag.assemble_seconds(),
            streamed.flag.write_seconds(),
            streamed.flag.bytes_written(),
        ),
        column_telemetry(
            "FLAG_CATEGORY",
            streamed.flag_category.assemble_seconds(),
            streamed.flag_category.write_seconds(),
            streamed.flag_category.bytes_written(),
        ),
        column_telemetry(
            "UVW",
            streamed.uvw.assemble_seconds(),
            streamed.uvw.write_seconds(),
            streamed.uvw.bytes_written(),
        ),
        column_telemetry(
            "WEIGHT",
            streamed.weight.assemble_seconds(),
            streamed.weight.write_seconds(),
            streamed.weight.bytes_written(),
        ),
        column_telemetry(
            "SIGMA",
            streamed.sigma.assemble_seconds(),
            streamed.sigma.write_seconds(),
            streamed.sigma.bytes_written(),
        ),
    ];
    MeasurementSetWriteTelemetry {
        assemble_seconds: columns.iter().map(|column| column.assemble_seconds).sum(),
        write_seconds: columns.iter().map(|column| column.write_seconds).sum(),
        bytes_written: columns.iter().map(|column| column.bytes_written).sum(),
        columns,
        rows_written: 0,
        maximum_resident_bytes: 0,
        producer_seconds: 0.0,
        queue_wait_seconds: 0.0,
        finalize_seconds: 0.0,
    }
}

fn complete_telemetry(
    mut telemetry: MeasurementSetWriteTelemetry,
    plan: &MeasurementSetWritePlan,
    producer_window_seconds: f64,
    queue_wait_seconds: f64,
    finalize_seconds: f64,
) -> MeasurementSetWriteTelemetry {
    telemetry.rows_written = plan.row_count;
    telemetry.maximum_resident_bytes = plan.maximum_resident_bytes;
    telemetry.producer_seconds = (producer_window_seconds - queue_wait_seconds).max(0.0);
    telemetry.queue_wait_seconds = queue_wait_seconds;
    telemetry.finalize_seconds = finalize_seconds;
    telemetry
}

fn duration_nanos_u64(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn column_telemetry(
    column: &str,
    assemble_seconds: f64,
    write_seconds: f64,
    bytes_written: usize,
) -> MeasurementSetColumnWriteTelemetry {
    MeasurementSetColumnWriteTelemetry {
        column: column.to_string(),
        assemble_seconds,
        write_seconds,
        bytes_written,
    }
}

fn install_streamed_columns(
    main: &casa_tables::Table,
    output: &Path,
    streamed: StreamedVisibilityColumns,
) -> Result<(), MeasurementSetWriteError> {
    let sequence = |column: &str| {
        crate::ms::measurement_set_main_data_manager_sequence(main, column).ok_or_else(|| {
            MeasurementSetWriteError::Install {
                column: column.to_string(),
                reason: "data-manager sequence is absent".to_string(),
            }
        })
    };
    install_streamed_tiled_shape_complex32_column(output, sequence("DATA")?, "DATA", streamed.data)
        .map_err(|error| install_error("DATA", error))?;
    install_streamed_tiled_shape_primitive_column(output, sequence("FLAG")?, "FLAG", streamed.flag)
        .map_err(|error| install_error("FLAG", error))?;
    install_streamed_tiled_shape_primitive_column(
        output,
        sequence("FLAG_CATEGORY")?,
        "FLAG_CATEGORY",
        streamed.flag_category,
    )
    .map_err(|error| install_error("FLAG_CATEGORY", error))?;
    install_streamed_tiled_column_primitive_column(output, sequence("UVW")?, "UVW", streamed.uvw)
        .map_err(|error| install_error("UVW", error))?;
    install_streamed_tiled_shape_primitive_column(
        output,
        sequence("WEIGHT")?,
        "WEIGHT",
        streamed.weight,
    )
    .map_err(|error| install_error("WEIGHT", error))?;
    install_streamed_tiled_shape_primitive_column(
        output,
        sequence("SIGMA")?,
        "SIGMA",
        streamed.sigma,
    )
    .map_err(|error| install_error("SIGMA", error))?;
    Ok(())
}

fn install_error(column: &str, error: impl std::fmt::Display) -> MeasurementSetWriteError {
    MeasurementSetWriteError::Install {
        column: column.to_string(),
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_uses_shape_budget_queue_and_tile_alignment() {
        let plan = MeasurementSetWritePlan::visibility_creation(
            1_000,
            4,
            64,
            "VLA",
            MeasurementSetWriteResources {
                available_bytes: 64 * 1024 * 1024,
                maximum_live_batches: 3,
                tiled_column_buffer_bytes: 0,
            },
        )
        .expect("write plan");
        assert!(plan.batch_rows > 0);
        assert!(
            plan.batch_rows == plan.row_count
                || plan.batch_rows % plan.visibility_tile_shape[2] == 0
        );
        assert!(plan.maximum_resident_bytes <= 64 * 1024 * 1024);
        assert!(plan.queue_capacity <= 2);
        assert_eq!(
            plan.columns
                .iter()
                .find(|column| column.name == "DATA")
                .expect("DATA plan")
                .storage_manager,
            MeasurementSetColumnStorage::TiledShape
        );
        assert_eq!(
            plan.columns
                .iter()
                .find(|column| column.name == "UVW")
                .expect("UVW plan")
                .storage_manager,
            MeasurementSetColumnStorage::TiledColumn
        );
        assert!(
            plan.columns
                .iter()
                .filter(|column| {
                    column.storage_manager == MeasurementSetColumnStorage::Standard
                })
                .count()
                >= 16
        );
    }

    #[test]
    fn plan_rejects_one_byte_below_one_row() {
        let row_plan = MeasurementSetWritePlan::visibility_creation(
            1,
            1,
            1,
            "VLA",
            MeasurementSetWriteResources {
                available_bytes: usize::MAX,
                maximum_live_batches: 1,
                tiled_column_buffer_bytes: 0,
            },
        )
        .expect("derive exact row bytes");
        let error = MeasurementSetWritePlan::visibility_creation(
            1,
            1,
            1,
            "VLA",
            MeasurementSetWriteResources {
                available_bytes: row_plan.maximum_resident_bytes - 1,
                maximum_live_batches: 1,
                tiled_column_buffer_bytes: 0,
            },
        )
        .expect_err("one row cannot fit");
        assert!(matches!(
            error,
            MeasurementSetWriteError::InsufficientBudget { .. }
        ));
    }

    #[test]
    fn visibility_plan_charges_every_explicit_column_io_buffer() {
        let plan_without_io_buffers = MeasurementSetWritePlan::visibility_creation(
            0,
            2,
            16,
            "VLA",
            MeasurementSetWriteResources {
                available_bytes: 256 * 1024 * 1024,
                maximum_live_batches: 2,
                tiled_column_buffer_bytes: 0,
            },
        )
        .expect("plan without I/O buffers");
        let per_column_buffer = 4 * 1024;
        let plan_with_io_buffers = MeasurementSetWritePlan::visibility_creation(
            0,
            2,
            16,
            "VLA",
            MeasurementSetWriteResources {
                available_bytes: 256 * 1024 * 1024,
                maximum_live_batches: 2,
                tiled_column_buffer_bytes: per_column_buffer,
            },
        )
        .expect("plan with I/O buffers");

        assert_eq!(
            plan_with_io_buffers.maximum_resident_bytes
                - plan_without_io_buffers.maximum_resident_bytes,
            6 * per_column_buffer
        );
    }

    #[test]
    fn visibility_session_rejects_a_batch_above_the_planned_boundary() {
        let mut plan = MeasurementSetWritePlan::visibility_creation(
            2,
            1,
            1,
            "VLA",
            MeasurementSetWriteResources {
                available_bytes: 64 * 1024 * 1024,
                maximum_live_batches: 1,
                tiled_column_buffer_bytes: 0,
            },
        )
        .expect("write plan");
        plan.batch_rows = 1;
        plan.batch_bytes = plan.bytes_per_row;
        let (sender, receiver) = mpsc::sync_channel(0);
        let session = MeasurementSetWriteSession {
            plan,
            state: MeasurementSetWriteSessionState::Creation {
                output: PathBuf::new(),
                sender,
                handle: thread::spawn(|| {
                    Err(MeasurementSetWriteError::Background(
                        "test writer stopped".to_string(),
                    ))
                }),
                sent_rows: AtomicUsize::new(0),
                scalar_writers: HashMap::new(),
                queue_wait_nanos: AtomicU64::new(0),
                started_at: Instant::now(),
            },
        };
        let error = session
            .send_batch(MeasurementSetWriteBatch {
                data_rows: vec![vec![Complex32::new(1.0, 0.0)]; 2],
                flag_rows: vec![false; 2],
                uvw_rows: vec![[0.0; 3]; 2],
            })
            .expect_err("oversized batch must be rejected");
        assert!(error.to_string().contains("plan permits at most 1"));
        drop(receiver);
    }

    #[test]
    fn columnar_plan_accounts_for_every_scalar_writer_buffer() {
        let scalar_buffer_bytes = 2 * STREAMING_SCALAR_COLUMN_BUFFER_BYTES;
        let arrays = vec![MeasurementSetArrayColumnPlan {
            name: "DATA".to_string(),
            value_type: StreamedTiledShapeValueType::Complex32,
            shapes: vec![MeasurementSetArrayShapePlan {
                cell_shape: vec![1, 1],
                row_count: 10,
                tile_shape: vec![1, 1, 10],
            }],
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        }];
        let scalars = vec![
            MeasurementSetScalarColumnPlan {
                name: "FLAG_ROW".to_string(),
                value_type: StreamedScalarType::Bool,
            },
            MeasurementSetScalarColumnPlan {
                name: "TIME".to_string(),
                value_type: StreamedScalarType::Float64,
            },
        ];
        let plan = MeasurementSetWritePlan::variable_array_creation(
            10,
            arrays.clone(),
            scalars.clone(),
            MeasurementSetWriteResources {
                available_bytes: scalar_buffer_bytes + 4 * 1024,
                maximum_live_batches: 2,
                tiled_column_buffer_bytes: 0,
            },
        )
        .expect("columnar plan");
        assert_eq!(plan.bytes_per_row, 8 + 2 + 9);
        assert_eq!(plan.columns.len(), 3);
        assert!(plan.maximum_resident_bytes >= scalar_buffer_bytes);
        assert!(plan.maximum_resident_bytes <= scalar_buffer_bytes + 4 * 1024);

        let error = MeasurementSetWritePlan::variable_array_creation(
            10,
            arrays,
            scalars,
            MeasurementSetWriteResources {
                available_bytes: scalar_buffer_bytes - 1,
                maximum_live_batches: 2,
                tiled_column_buffer_bytes: 0,
            },
        )
        .expect_err("fixed scalar buffers must fit");
        assert!(error.to_string().contains("scalar column buffers require"));
    }

    #[test]
    fn variable_array_plan_requires_one_fixed_shape_for_tiled_column() {
        let resources = MeasurementSetWriteResources {
            available_bytes: 64 * 1024 * 1024,
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: 0,
        };
        let fixed = MeasurementSetArrayColumnPlan {
            name: "UVW".to_string(),
            value_type: StreamedTiledShapeValueType::Float64,
            shapes: vec![MeasurementSetArrayShapePlan {
                cell_shape: vec![3],
                row_count: 10,
                tile_shape: vec![3, 10],
            }],
            storage_manager: MeasurementSetColumnStorage::TiledColumn,
        };

        let plan = MeasurementSetWritePlan::variable_array_creation(
            10,
            vec![fixed.clone()],
            Vec::new(),
            resources,
        )
        .expect("fixed-shape tiled column plan");
        assert_eq!(
            plan.array_columns[0].storage_manager,
            MeasurementSetColumnStorage::TiledColumn
        );

        let mut heterogeneous = fixed;
        heterogeneous.shapes = vec![
            MeasurementSetArrayShapePlan {
                cell_shape: vec![3],
                row_count: 5,
                tile_shape: vec![3, 5],
            },
            MeasurementSetArrayShapePlan {
                cell_shape: vec![4],
                row_count: 5,
                tile_shape: vec![4, 5],
            },
        ];
        let error = MeasurementSetWritePlan::variable_array_creation(
            10,
            vec![heterogeneous],
            Vec::new(),
            resources,
        )
        .expect_err("heterogeneous tiled column must be rejected");
        assert!(
            error
                .to_string()
                .contains("requires one defined shape for every row")
        );
    }

    #[test]
    fn variable_shape_plans_stay_bounded_for_realistic_and_skewed_cells() {
        let cases = [
            ("small", vec![(vec![1, 16], 12_000)]),
            ("medium", vec![(vec![2, 128], 80_000)]),
            ("large", vec![(vec![4, 4_096], 25_000)]),
            (
                "skewed",
                vec![
                    (vec![1, 1], 90_000),
                    (vec![2, 64], 9_900),
                    (vec![4, 8_192], 100),
                ],
            ),
        ];
        let available_bytes = 256 * 1024 * 1024;

        for (name, shapes) in cases {
            let row_count = shapes.iter().map(|(_, rows)| rows).sum();
            let shapes = shapes
                .into_iter()
                .map(|(cell_shape, row_count)| MeasurementSetArrayShapePlan {
                    tile_shape: MeasurementSetArrayShapePlan::visibility(
                        cell_shape[0],
                        cell_shape[1],
                        row_count,
                        "VLA",
                    )
                    .tile_shape,
                    cell_shape,
                    row_count,
                })
                .collect();
            let plan = MeasurementSetWritePlan::variable_array_creation(
                row_count,
                vec![MeasurementSetArrayColumnPlan {
                    name: "DATA".to_string(),
                    value_type: StreamedTiledShapeValueType::Complex32,
                    shapes,
                    storage_manager: MeasurementSetColumnStorage::TiledShape,
                }],
                standard_main_scalar_column_plans(),
                MeasurementSetWriteResources {
                    available_bytes,
                    maximum_live_batches: 3,
                    tiled_column_buffer_bytes: 0,
                },
            )
            .unwrap_or_else(|error| panic!("{name} shape plan failed: {error}"));

            assert!(plan.batch_rows > 0, "{name}");
            assert!(
                plan.maximum_resident_bytes <= available_bytes,
                "{name}: {} > {available_bytes}",
                plan.maximum_resident_bytes
            );
        }
    }

    #[test]
    fn create_target_preserves_existing_output_until_commit() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let final_path = directory.path().join("output.ms");
        fs::create_dir(&final_path).expect("existing output");
        fs::write(final_path.join("sentinel"), b"complete").expect("sentinel");

        let target = MeasurementSetCreateTarget::prepare(&final_path, true).expect("target");
        assert!(final_path.join("sentinel").exists());
        fs::create_dir(target.staging_path()).expect("staging output");
        fs::write(target.staging_path().join("replacement"), b"complete").expect("replacement");
        target.commit().expect("publish replacement");

        assert!(!final_path.join("sentinel").exists());
        assert!(final_path.join("replacement").exists());
    }

    #[test]
    fn uncommitted_create_target_is_detectably_incomplete() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let final_path = directory.path().join("output.ms");
        let target = MeasurementSetCreateTarget::prepare(&final_path, false).expect("target");
        fs::create_dir(target.staging_path()).expect("staging output");

        assert!(!final_path.exists());
        assert!(
            target
                .staging_path()
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains("casa-rs-incomplete"))
        );
    }
}
