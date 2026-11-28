// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

mod array_shapes;
mod array_sources;
pub(crate) mod asdm_stman;
pub(crate) mod canonical;
mod cell_elements;
pub(crate) mod column_changes;
mod data_file;
pub(crate) mod data_type;
mod fixed_arrays;
pub(crate) mod incremental_stman;
pub(crate) mod multifile;
pub(crate) mod row_changes;
mod scalar_sources;
mod selected_cells;
pub(crate) mod standard_stman;
pub(crate) mod stman_aipsio;
pub(crate) mod stman_array_file;
pub(crate) mod table_control;
pub(crate) mod table_keyword_write;
mod tile_layout;
pub(crate) mod tiled_stman;
pub use tile_layout::{DEFAULT_TILE_IO_BYTES, TileLayoutError, TileLayoutPlan, TileLayoutPlanner};
pub use tiled_stman::{
    DEFAULT_TILED_ARRAY_CACHE_BYTES, STREAMING_TILED_COLUMN_BUFFER_BYTES,
    StreamedTiledPrimitiveColumn, StreamedTiledPrimitiveType, StreamedTiledShapeColumn,
    StreamedTiledShapeComplex32Column, StreamedTiledShapeCubeLayout, StreamedTiledShapeValueType,
    StreamingTiledPrimitiveWriter, StreamingTiledShapeComplex32Writer, StreamingTiledShapeWriter,
    TilePixel, TiledArrayStorage, TiledArrayStorageLayout, TiledFileIoStats,
    install_streamed_tiled_column, install_streamed_tiled_column_primitive_column,
    install_streamed_tiled_shape_column, install_streamed_tiled_shape_complex32_column,
    install_streamed_tiled_shape_primitive_column, install_undefined_tiled_shape_column,
    set_table_cache_budget_bytes, table_cache_budget_bytes,
};
mod concat_projection;
pub(crate) mod virtual_bitflags;
pub(crate) mod virtual_compress;
pub(crate) mod virtual_engine;
pub(crate) mod virtual_forward;
pub(crate) mod virtual_scaled_array;
pub(crate) mod virtual_taql_column;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_aipsio::ByteOrder;
use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use thiserror::Error;

use crate::schema::{SchemaError, TableSchema};
use crate::table::{
    ColumnOverride, ColumnOverrides, RequiredScalarColumnDestination,
    RequiredScalarColumnValuesMut, SelectedArray1DCells, SelectedArray1DCellsMut,
    SelectedArray1DShape, SelectedArray2DCells, SelectedArray2DCellsMut, SelectedArray2DShape,
    SelectedReadFootprint,
};
use crate::table_path::{add_directory_for_link, rebase_table_keywords};

use self::array_sources::generated_array_group_sources;
use self::incremental_stman::{
    IsmColumnResult, read_ism_array_column_rows, read_ism_file, read_ism_file_columns,
    read_ism_required_scalar_columns_rows, read_ism_required_scalar_columns_rows_reusing,
    read_ism_scalar_column, read_ism_scalar_column_rows, write_ism_file,
    write_ism_file_column_sources, write_ism_file_indexed,
};
use self::scalar_sources::override_columns_for_group;
pub use self::scalar_sources::streamed_scalar_columns_resident_bytes;
pub(crate) use self::scalar_sources::{ColumnSources, EncodedCells};
pub(crate) use self::selected_cells::{cell_read_footprint, channel_read_footprint};
use self::standard_stman::{
    read_ssm_array_column_rows, read_ssm_file, read_ssm_file_columns,
    read_ssm_required_scalar_columns_rows, read_ssm_required_scalar_columns_rows_reusing,
    read_ssm_scalar_column_rows, write_ssm_file_column_sources, write_ssm_file_indexed,
    write_ssm_file_with_options,
};
use self::stman_aipsio::scalar_value_is_default;
use self::stman_aipsio::{
    ColumnRawData, StManColumnData, StManColumnInfo, extract_row_value,
    read_stman_array_column_rows, read_stman_file, read_stman_scalar_column,
    read_stman_scalar_column_rows, write_stman_file,
};
pub(crate) use self::table_control::{
    RefTableDatContents, TableDatContents, TableDatResult, read_table_dat_dispatch,
};
pub(crate) use self::tiled_stman::TiledReadMetadata;
use self::virtual_compress::{ComplexCompression, ComplexScale};
use self::virtual_engine::{VirtualContext, is_virtual_engine, lookup_engine};

use self::table_control::{StagedTableDat, write_concat_table_dat, write_ref_table_dat};

mod array_read;
pub(crate) mod full_save;
mod load_columns;
mod metadata;
mod save;
mod scalar_read;
mod scalar_values;

pub(crate) use load_columns::undefined_indirect_cell;
use load_columns::*;
use metadata::*;
use scalar_values::*;

pub(crate) const TABLE_CONTROL_FILE: &str = "table.dat";
pub(crate) const TABLE_DATA_FILE_PREFIX: &str = "table.f";
pub(crate) const TABLE_INFO_FILE: &str = "table.info";

struct SelectedArray2DChannelRead<'a> {
    column: &'a str,
    selected_rows: &'a [usize],
    channel_start: usize,
    channel_count: usize,
}

struct SelectedArray1DRead<'a> {
    column: &'a str,
    selected_rows: &'a [usize],
}

#[derive(Clone, Debug)]
pub(crate) struct RetainedTableReadMetadata {
    pub(crate) table_dat: TableDatContents,
    pub(crate) tiled: TiledReadMetadata,
}

impl RetainedTableReadMetadata {
    pub(crate) fn open(
        table_path: &Path,
        table_dat: TableDatContents,
    ) -> Result<Self, StorageError> {
        if table_dat.uses_multifile()
            && table_dat
                .column_set
                .data_managers
                .iter()
                .any(|manager| multifile::supports_multifile(&manager.type_name))
        {
            let container = table_path.join("table.mf");
            if !container.is_file() {
                return Err(StorageError::MissingDataFile(container));
            }
        }
        table_dat.asdm.retain_indexes()?;
        let tiled = TiledReadMetadata::open(table_path, &table_dat.column_set.data_managers)?;
        Ok(Self { table_dat, tiled })
    }

    pub(crate) fn retained_heap_bytes(&self) -> Option<usize> {
        self.table_dat
            .retained_heap_bytes()?
            .checked_add(self.tiled.retained_heap_bytes()?)
    }
}

fn reorder_row_to_requested_columns(row: &RecordValue, columns: &[&str]) -> RecordValue {
    RecordValue::new(
        columns
            .iter()
            .filter_map(|column| {
                row.get(column)
                    .cloned()
                    .map(|value| RecordField::new(*column, value))
            })
            .collect(),
    )
}

/// The `tracing` target of the storage save and load profile (`debug`).
const STORAGE_PROFILE: &str = "casa_tables::storage_profile";
/// The `tracing` target of the table-open stage profile (`debug`).
pub(crate) const TABLE_OPEN_PROFILE: &str = "casa_tables::table_open_profile";

/// Whether the logging configuration takes the storage profile; when it
/// does not, the profiler takes no clock.
fn storage_profile_enabled() -> bool {
    tracing::enabled!(target: STORAGE_PROFILE, tracing::Level::DEBUG)
}

/// Whether the logging configuration takes the table-open stage profile.
pub(crate) fn table_open_profile_enabled() -> bool {
    tracing::enabled!(target: TABLE_OPEN_PROFILE, tracing::Level::DEBUG)
}

fn log_storage_profile(context: &str, phase: &str, delta: f64, total: f64, detail: Option<&str>) {
    let mut line =
        format!("[casa-tables profile] {context} phase={phase} dt={delta:.3}s total={total:.3}s");
    if let Some(detail) = detail
        && !detail.is_empty()
    {
        line.push(' ');
        line.push_str(detail);
    }
    tracing::debug!(target: STORAGE_PROFILE, "{line}");
}

pub(crate) struct StorageProfiler {
    context: String,
    start: Instant,
    last: Instant,
}

impl StorageProfiler {
    pub(crate) fn start(context: impl Into<String>) -> Option<Self> {
        if !storage_profile_enabled() {
            return None;
        }
        let now = Instant::now();
        Some(Self {
            context: context.into(),
            start: now,
            last: now,
        })
    }

    pub(crate) fn mark(&mut self, phase: &str) {
        self.mark_with_detail(phase, None::<String>);
    }

    pub(crate) fn mark_with_detail(&mut self, phase: &str, detail: impl Into<Option<String>>) {
        let now = Instant::now();
        let delta = now.duration_since(self.last).as_secs_f64();
        let total = now.duration_since(self.start).as_secs_f64();
        self.last = now;
        let detail = detail.into();
        log_storage_profile(&self.context, phase, delta, total, detail.as_deref());
    }
}

/// Errors arising from table storage operations (I/O, format, schema).
///
/// C++ equivalent: various exceptions in `DataManager.h` and `Table.h`.
#[derive(Debug, Error)]
pub enum StorageError {
    /// The table directory does not exist on the filesystem.
    #[error("table path does not exist: {0}")]
    MissingPath(PathBuf),
    /// The `table.dat` control file is absent.
    #[error("table control file is missing: {0}")]
    MissingControlFile(PathBuf),
    /// A required `table.f*` data file is absent.
    #[error("table data file is missing: {0}")]
    MissingDataFile(PathBuf),
    /// On-disk format does not match the expected version or layout.
    #[error("format mismatch: {0}")]
    FormatMismatch(String),
    /// Column schema validation failed.
    #[error("schema error: {0}")]
    Schema(String),
    /// The data manager type is not recognised or not yet implemented.
    #[error("unsupported data manager: {0}")]
    UnsupportedDataManager(String),
    /// A fixed-shape array column bound to IncrementalStMan without the
    /// Direct option. casacore stores such a column as an `ISMIndColumn`,
    /// its cells in the `table.fNi` array file, which casa-rs does not
    /// write.
    #[error(
        "IncrementalStMan array column {0} is not Direct: casacore stores it as an \
         ISMIndColumn in table.fNi, which casa-rs does not write; make the column \
         Direct or bind it to another data manager"
    )]
    IsmIndirectArrayColumn(String),
    /// An underlying I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// An error from the AipsIO serialisation layer.
    #[error("aipsio error: {0}")]
    AipsIo(#[from] casa_aipsio::AipsIoObjectError),
}

impl From<SchemaError> for StorageError {
    fn from(value: SchemaError) -> Self {
        Self::Schema(value.to_string())
    }
}

/// C++ `TableInfo` metadata stored in `table.info`.
///
/// Contains the logical table type (e.g. `"MeasurementSet"`) and subtype
/// (e.g. `"UVFITS"`) written as plain-text key-value pairs.
///
/// # C++ equivalent
///
/// `casacore::TableInfo` — persisted via `TableInfo::flush()` / `readBack()`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableInfo {
    /// Logical table type (e.g. `"MeasurementSet"`). Empty if unset.
    pub table_type: String,
    /// Table subtype (e.g. `"UVFITS"`). Empty if unset.
    pub sub_type: String,
    /// Free-form readme lines following the type/subtype header.
    pub readme: Vec<String>,
}

impl TableInfo {
    /// Parse a `table.info` file from its text contents.
    pub fn parse(contents: &str) -> Self {
        let mut table_type = String::new();
        let mut sub_type = String::new();
        let mut readme = Vec::new();
        let mut in_readme = false;
        for line in contents.lines() {
            if in_readme {
                readme.push(line.to_string());
            } else if line.is_empty() {
                in_readme = true;
            } else if let Some(rest) = line.strip_prefix("Type = ") {
                table_type = rest.to_string();
            } else if let Some(rest) = line.strip_prefix("SubType = ") {
                sub_type = rest.to_string();
            }
        }
        Self {
            table_type,
            sub_type,
            readme,
        }
    }
}

impl std::fmt::Display for TableInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Type = {}\nSubType = {}\n",
            self.table_type, self.sub_type
        )?;
        if !self.readme.is_empty() {
            writeln!(f)?;
            for line in &self.readme {
                writeln!(f, "{line}")?;
            }
        }
        Ok(())
    }
}

/// Metadata for one data manager instance.
///
/// Each data manager manages one or more columns. The sequence number
/// is unique within a table and identifies the on-disk data file
/// (`table.f<seq_nr>`).
///
/// # C++ equivalent
///
/// `DataManager::dataManagerName()`, `DataManager::dataManagerType()`,
/// `DataManager::sequenceNr()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataManagerInfo {
    /// The data manager type name (e.g. `"StManAipsIO"`, `"StandardStMan"`).
    pub dm_type: String,
    /// The manager's name: the data manager group its columns are declared
    /// with (empty when they name none), as `DataManager::dataManagerName()`.
    pub name: String,
    /// Unique sequence number within the table.
    pub seq_nr: u32,
    /// Column names managed by this data manager.
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageSnapshot {
    pub(crate) row_count: usize,
    pub(crate) rows: Vec<RecordValue>,
    /// per-row set of column names that are undefined cells
    pub(crate) undefined_cells: Vec<HashSet<String>>,
    pub(crate) keywords: RecordValue,
    pub(crate) column_keywords: HashMap<String, RecordValue>,
    pub(crate) schema: Option<TableSchema>,
    pub(crate) table_info: TableInfo,
    /// Names of columns materialized by virtual engines (empty for non-virtual tables).
    pub(crate) virtual_columns: HashSet<String>,
    /// Virtual column bindings for save (empty on load).
    pub(crate) virtual_bindings: Vec<virtual_engine::VirtualColumnBinding>,
    /// Data manager info extracted from table.dat (empty for memory tables).
    pub(crate) dm_info: Vec<DataManagerInfo>,
    /// Parsed immutable storage metadata retained by a lazy plain-table open.
    pub(crate) retained_read_metadata: Option<RetainedTableReadMetadata>,
}

#[derive(Debug, Clone)]
pub(crate) struct ScalarColumnSnapshot {
    pub(crate) row_count: usize,
    pub(crate) columns: HashMap<String, Vec<Option<ScalarValue>>>,
}

#[derive(Debug, Clone)]
pub(crate) enum RequiredScalarColumnData {
    Bool(Vec<bool>),
    Int32(Vec<i32>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
}

#[derive(Debug, Clone)]
pub(crate) struct RequiredScalarColumnSnapshot {
    pub(crate) row_count: usize,
    pub(crate) columns: HashMap<String, RequiredScalarColumnData>,
}

fn filter_rows_for_save(
    rows: &[RecordValue],
    undefined_cells: &[HashSet<String>],
) -> Vec<RecordValue> {
    rows.iter()
        .enumerate()
        .map(|(idx, row)| {
            let undef = undefined_cells.get(idx);
            let fields: Vec<_> = row
                .fields()
                .iter()
                .filter(|f| undef.is_none_or(|u| !u.contains(&f.name)))
                .cloned()
                .collect();
            RecordValue::new(fields)
        })
        .collect()
}

struct RowFieldReorder {
    names: Vec<String>,
    swaps: Vec<(usize, usize)>,
}

fn full_projection_layout(
    fields: &[RecordField],
    names: &[(&String, &String)],
) -> Option<RowFieldReorder> {
    let layout: Vec<_> = fields.iter().map(|field| field.name.clone()).collect();
    let wanted: Vec<_> = names
        .iter()
        .filter_map(|(_, name)| layout.iter().position(|present| present == *name))
        .collect();
    if wanted.len() != layout.len() {
        return None;
    }
    let mut indices: Vec<_> = (0..layout.len()).collect();
    let mut swaps = Vec::new();
    for (target, original) in wanted.into_iter().enumerate() {
        let current = indices
            .iter()
            .position(|index| *index == original)
            .expect("actual field is in the layout");
        if current != target {
            indices.swap(current, target);
            swaps.push((current, target));
        }
    }
    Some(RowFieldReorder {
        names: layout,
        swaps,
    })
}

fn override_value_for_row(
    column_overrides: &ColumnOverrides,
    column: &str,
    row_idx: usize,
) -> Option<Value> {
    match column_overrides.get(column)? {
        ColumnOverride::Values(values) => values.get(row_idx).cloned().unwrap_or(None),
        ColumnOverride::GeneratedScalar(column) => column.value(row_idx).map(Value::Scalar),
        ColumnOverride::GeneratedArray(column) => column.value(row_idx).map(Value::Array),
        ColumnOverride::StreamedScalar(_) => None,
        ColumnOverride::Deferred => None,
    }
}

fn project_rows_for_group(
    rows: &[RecordValue],
    row_count: usize,
    group_col_descs: &[table_control::ColumnDescContents],
    group_col_indices: Option<&[usize]>,
    column_overrides: &ColumnOverrides,
) -> Vec<RecordValue> {
    (0..row_count)
        .map(|row_idx| {
            let row = rows.get(row_idx);
            let fields = group_col_descs
                .iter()
                .enumerate()
                .filter_map(|(col_idx, desc)| {
                    let value = override_value_for_row(column_overrides, &desc.col_name, row_idx)
                        .or_else(|| {
                            group_col_indices
                                .and_then(|indices| {
                                    row.and_then(|row| {
                                        row.fields()
                                            .get(indices[col_idx])
                                            .filter(|field| field.name == desc.col_name)
                                            .map(|field| field.value.clone())
                                    })
                                })
                                .or_else(|| row.and_then(|row| row.get(&desc.col_name).cloned()))
                        });
                    value.map(|value| RecordField::new(desc.col_name.clone(), value))
                })
                .collect();
            RecordValue::new(fields)
        })
        .collect()
}

fn project_column_values_for_group<'a>(
    rows: &'a [RecordValue],
    col_name: &str,
    field_index: Option<usize>,
) -> Vec<Option<&'a Value>> {
    rows.iter()
        .map(|row| {
            if let Some(idx) = field_index
                && let Some(field) = row.fields().get(idx).filter(|field| field.name == col_name)
            {
                return Some(&field.value);
            }
            row.get(col_name)
        })
        .collect()
}

/// Composite storage manager that dispatches per data manager type.
///
/// Reads `table.dat` once, then routes each DM's columns to the appropriate
/// reader (StManAipsIO or StandardStMan). Writes use StManAipsIO by default.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct CompositeStorage;

impl CompositeStorage {
    pub(crate) fn load(&self, table_path: &Path) -> Result<StorageSnapshot, StorageError> {
        self.load_with_row_hint(table_path, None)
    }
}

fn column_is_virtual(table_dat: &TableDatContents, column: &str) -> bool {
    let Some(binding) = table_dat
        .column_set
        .columns
        .iter()
        .find(|entry| entry.original_name == column)
    else {
        return false;
    };
    table_dat
        .column_set
        .data_managers
        .iter()
        .any(|manager| manager.seq_nr == binding.dm_seq_nr && is_virtual_engine(&manager.type_name))
}

#[cfg(test)]
mod tests;
