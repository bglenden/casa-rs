// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded columnar MeasurementSet write planning and execution.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use casa_core::{ArrayValue, PrimitiveType, ScalarValue, Value};
use casa_tables::streamed_tiled::{
    STREAMING_TILED_COLUMN_BUFFER_BYTES, StreamedTiledPrimitiveColumn, StreamedTiledPrimitiveType,
    StreamedTiledShapeComplex32Column, StreamedTiledShapeValueType, StreamingTiledPrimitiveWriter,
    StreamingTiledShapeComplex32Writer, StreamingTiledShapeWriter, install_streamed_tiled_column,
    install_streamed_tiled_column_primitive_column, install_streamed_tiled_shape_column,
    install_streamed_tiled_shape_complex32_column, install_streamed_tiled_shape_primitive_column,
    install_undefined_tiled_shape_column,
};
use casa_tables::{
    ColumnBinding, ColumnOverrides, StreamedScalarType, Table, TableError, TableOptions,
    TableWriteLock, streamed_scalar_columns_resident_bytes,
};
use num_complex::Complex32;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{MeasurementSet, MsSelectionIoBudget, SubtableColumnOverrides};

mod scalars;
mod variable_budget;
mod visibility_columns;
mod visibility_destination;

pub use scalars::ScalarColumnId;
use scalars::ScalarWriters;
pub use visibility_columns::VisibilityColumns;
pub use visibility_destination::VisibilityWriteDestination;

mod plan;
mod session;

/// Bytes a visibility creation batch aims at, whatever the budget allows.
pub const TARGET_BATCH_BYTES: usize = 32 * 1024 * 1024;

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

/// Bytes the visibility columns' streamed tiled writers hold for cells of
/// `(correlations, channels)` with the visibility, UVW and weight tile
/// shapes `tiles`: a band of tiles and an I/O buffer of `buffer_bytes`
/// each, DATA's with `data_bands` bands, and the WEIGHT and SIGMA rows.
fn visibility_writer_resident_bytes(
    (correlations, channels): (usize, usize),
    [visibility_tiles, uvw_tiles, weight_tiles]: [&[usize]; 3],
    data_bands: usize,
    buffer_bytes: usize,
) -> Result<usize, MeasurementSetWriteError> {
    let cell = [correlations, channels];
    [
        (&cell[..], visibility_tiles, 8 * data_bands),
        (&cell[..], visibility_tiles, 1),
        (&[3][..], uvw_tiles, 8),
        (&[correlations][..], weight_tiles, 4),
        (&[correlations][..], weight_tiles, 4),
    ]
    .into_iter()
    .try_fold(0usize, |total, (cell_shape, tile_shape, element_bytes)| {
        let bytes = sequential_tiled_writer_resident_bytes(
            cell_shape,
            tile_shape,
            element_bytes,
            buffer_bytes,
        )?;
        total
            .checked_add(bytes)
            .ok_or(MeasurementSetWriteError::ByteOverflow)
    })?
    .checked_add(
        correlations
            .checked_mul(2 * std::mem::size_of::<f32>())
            .ok_or(MeasurementSetWriteError::ByteOverflow)?,
    )
    .ok_or(MeasurementSetWriteError::ByteOverflow)
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
    /// Bands of tiles DATA's streamed writer holds: two when the budget
    /// lets it lay one out while the other is written, else one.
    data_bands: usize,
    /// Explicit selected MAIN rows for mutation plans.
    selected_rows: Vec<usize>,
    /// Exact columns owned by the operation.
    columns: Vec<MeasurementSetWriteColumnPlan>,
    array_columns: Vec<MeasurementSetArrayColumnPlan>,
    array_column_memory_budget_bytes: usize,
    scalar_columns: Vec<MeasurementSetScalarColumnPlan>,
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
pub(crate) enum MeasurementSetWriteBatch {
    Columns(VisibilityColumns),
    #[cfg(test)]
    Repeated {
        data_row: Vec<Complex32>,
        flag_row: bool,
        uvw_row: [f64; 3],
        row_count: usize,
    },
}

impl MeasurementSetWriteBatch {
    fn row_count(&self) -> usize {
        match self {
            Self::Columns(columns) => columns.row_count(),
            #[cfg(test)]
            Self::Repeated { row_count, .. } => *row_count,
        }
    }
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
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
    /// Flush, installation, lock release, and other finalization time.
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
    /// casacore's write lock on MAIN could not be taken or released.
    ///
    /// [`TableError::LockFailed`] means another handle in this process holds
    /// it, another process wrote MAIN while this writer waited for it, or
    /// waiting would deadlock; a lock another process holds is waited for.
    #[error("casacore write lock on MeasurementSet {path}: {source}")]
    WriteLock {
        /// MeasurementSet (MAIN table) directory.
        path: String,
        /// Lock failure.
        #[source]
        source: TableError,
    },
    /// A mutation batch was given a MeasurementSet other than the one the
    /// session locked and was started on.
    #[error(
        "selected-row mutation of MeasurementSet {session} was given MeasurementSet {batch}; \
         a session writes only the MeasurementSet it locked"
    )]
    ForeignMeasurementSet {
        /// MeasurementSet directory the session locked.
        session: String,
        /// MeasurementSet directory the batch was given.
        batch: String,
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
        scalar_writers: ScalarWriters,
        queue_wait_nanos: AtomicU64,
        started_at: Instant,
        /// Written batches the writer hands back for reuse, at most one.
        recycled: mpsc::Receiver<VisibilityColumns>,
    },
    VariableArrayCreation {
        output: PathBuf,
        writers: HashMap<String, StreamingTiledShapeWriter>,
        scalar_writers: ScalarWriters,
        started_at: Instant,
    },
    Mutation {
        /// casacore's MAIN write lock, held until the session finishes or is
        /// dropped.
        write_lock: Option<TableWriteLock>,
        /// Canonical path of the locked MeasurementSet; every batch must
        /// write this one.
        target: PathBuf,
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

/// The canonical path of a MeasurementSet directory, which identifies the
/// MeasurementSet a mutation session writes.
fn canonical_measurement_set_path(path: &Path) -> Result<PathBuf, MeasurementSetWriteError> {
    std::fs::canonicalize(path).map_err(|error| {
        MeasurementSetWriteError::InvalidPlan(format!(
            "resolve MeasurementSet {}: {error}",
            path.display()
        ))
    })
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
    returns: &mpsc::SyncSender<VisibilityColumns>,
    mut data_writer: StreamingTiledShapeComplex32Writer,
    mut flag_writer: StreamingTiledPrimitiveWriter,
    mut uvw_writer: StreamingTiledPrimitiveWriter,
    mut weight_writer: StreamingTiledPrimitiveWriter,
    mut sigma_writer: StreamingTiledPrimitiveWriter,
    correlation_count: usize,
) -> Result<StreamedVisibilityColumns, MeasurementSetWriteError> {
    let weight_row = vec![1.0f32; correlation_count];
    let sigma_row = vec![1.0f32; correlation_count];
    for batch in receiver {
        match batch {
            MeasurementSetWriteBatch::Columns(columns) => {
                let row_count = columns.row_count();
                data_writer
                    .push_rows(&columns.data)
                    .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                flag_writer
                    .push_bool_fill_rows(&columns.flag_row)
                    .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                uvw_writer
                    .push_f64_rows(&columns.uvw)
                    .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                visibility_columns::push_per_row_cells(
                    &mut weight_writer,
                    &columns.weight,
                    &weight_row,
                    row_count,
                )?;
                visibility_columns::push_per_row_cells(
                    &mut sigma_writer,
                    &columns.sigma,
                    &sigma_row,
                    row_count,
                )?;
                visibility_columns::recycle(returns, columns);
            }
            #[cfg(test)]
            MeasurementSetWriteBatch::Repeated {
                data_row,
                flag_row,
                uvw_row,
                row_count,
            } => {
                data_writer
                    .push_repeated_row(&data_row, row_count)
                    .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                flag_writer
                    .push_repeated_bool_fill_row(flag_row, row_count)
                    .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                uvw_writer
                    .push_repeated_f64_row(&uvw_row, row_count)
                    .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                weight_writer
                    .push_repeated_f32_row(&weight_row, row_count)
                    .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                sigma_writer
                    .push_repeated_f32_row(&sigma_row, row_count)
                    .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            }
        }
    }
    Ok(StreamedVisibilityColumns {
        data: data_writer
            .finish()
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?,
        flag: flag_writer
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
        // FLAG_CATEGORY's cells are undefined: nothing is assembled or written.
        column_telemetry("FLAG_CATEGORY", 0.0, 0.0, 0),
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
    plan: &MeasurementSetWritePlan,
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
    // Undefined cells, laid out as casacore leaves a column no row has
    // written; the default tile is the one a later hypercube would get.
    let visibility_tile = &plan.visibility_tile_shape;
    install_undefined_tiled_shape_column(
        output,
        sequence("FLAG_CATEGORY")?,
        "FLAG_CATEGORY",
        plan.row_count,
        StreamedTiledPrimitiveType::Bool,
        3,
        &[
            visibility_tile[0],
            visibility_tile[1],
            1,
            visibility_tile[2],
        ],
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
mod tests;

#[cfg(test)]
mod throughput_tests;
