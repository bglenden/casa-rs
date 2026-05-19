// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

pub(crate) mod canonical;
pub(crate) mod data_type;
pub(crate) mod incremental_stman;
pub(crate) mod standard_stman;
pub(crate) mod stman_aipsio;
pub(crate) mod stman_array_file;
pub(crate) mod table_control;
pub(crate) mod tiled_stman;
pub use tiled_stman::{
    StreamedTiledPrimitiveColumn, StreamedTiledPrimitiveType, StreamedTiledShapeComplex32Column,
    StreamingTiledPrimitiveWriter, StreamingTiledShapeComplex32Writer, TilePixel, TiledFileIO,
    install_streamed_tiled_column_primitive_column, install_streamed_tiled_shape_complex32_column,
    install_streamed_tiled_shape_primitive_column, set_table_cache_budget_bytes,
    table_cache_budget_bytes,
};
pub(crate) mod virtual_bitflags;
pub(crate) mod virtual_compress;
pub(crate) mod virtual_engine;
pub(crate) mod virtual_forward;
pub(crate) mod virtual_scaled_array;
pub(crate) mod virtual_taql_column;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Instant;

use casa_aipsio::ByteOrder;
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{Axis, Slice};
use thiserror::Error;

use crate::schema::{SchemaError, TableSchema};

use self::data_type::CasacoreDataType;
use self::incremental_stman::{
    IsmColumnResult, read_ism_file, read_ism_scalar_column, read_ism_scalar_column_rows,
    write_ism_file, write_ism_file_indexed, write_ism_file_scalar_columns,
};
use self::standard_stman::{
    read_ssm_array_column_rows, read_ssm_file, read_ssm_scalar_column_rows, write_ssm_file,
    write_ssm_file_indexed, write_ssm_file_scalar_columns,
};
use self::stman_aipsio::scalar_value_is_default;
use self::stman_aipsio::{
    StManColumnData, StManColumnInfo, extract_row_value, read_stman_array_column_rows,
    read_stman_file, read_stman_scalar_column, read_stman_scalar_column_rows, write_stman_file,
};
pub(crate) use self::table_control::RefTableDatContents;
use self::virtual_engine::{VirtualContext, is_virtual_engine, lookup_engine};

use self::table_control::{
    TableDatContents, TableDatResult, read_table_dat_dispatch, write_concat_table_dat,
    write_ref_table_dat, write_table_dat,
};

pub(crate) const TABLE_CONTROL_FILE: &str = "table.dat";
pub(crate) const TABLE_DATA_FILE_PREFIX: &str = "table.f";
pub(crate) const TABLE_INFO_FILE: &str = "table.info";

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

fn storage_profile_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("CASA_RS_STORAGE_PROFILE") {
        Ok(value) => {
            let trimmed = value.trim();
            !trimmed.is_empty()
                && trimmed != "0"
                && !trimmed.eq_ignore_ascii_case("false")
                && !trimmed.eq_ignore_ascii_case("off")
        }
        Err(_) => false,
    })
}

fn log_storage_profile(context: &str, phase: &str, delta: f64, total: f64, detail: Option<&str>) {
    let mut line =
        format!("[casa-tables profile] {context} phase={phase} dt={delta:.3}s total={total:.3}s");
    if let Some(detail) = detail {
        if !detail.is_empty() {
            line.push(' ');
            line.push_str(detail);
        }
    }
    eprintln!("{line}");
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
}

impl TableInfo {
    /// Parse a `table.info` file from its text contents.
    pub fn parse(contents: &str) -> Self {
        let mut table_type = String::new();
        let mut sub_type = String::new();
        for line in contents.lines() {
            if let Some(rest) = line.strip_prefix("Type = ") {
                table_type = rest.to_string();
            } else if let Some(rest) = line.strip_prefix("SubType = ") {
                sub_type = rest.to_string();
            }
        }
        Self {
            table_type,
            sub_type,
        }
    }
}

impl std::fmt::Display for TableInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Type = {}\nSubType = {}\n",
            self.table_type, self.sub_type
        )
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
}

#[derive(Debug, Clone)]
pub(crate) struct ScalarColumnSnapshot {
    pub(crate) row_count: usize,
    pub(crate) columns: HashMap<String, Vec<Option<ScalarValue>>>,
}

pub(crate) trait StorageManager {
    fn load(&self, table_path: &Path) -> Result<StorageSnapshot, StorageError>;
    fn save(
        &self,
        table_path: &Path,
        snapshot: &StorageSnapshot,
        dm_kind: crate::table::DataManagerKind,
        big_endian: bool,
        tile_shape: Option<&[usize]>,
    ) -> Result<(), StorageError>;
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

fn project_rows_for_group(
    rows: &[RecordValue],
    group_col_descs: &[table_control::ColumnDescContents],
    group_col_indices: Option<&[usize]>,
    column_overrides: &HashMap<String, Vec<Option<Value>>>,
) -> Vec<RecordValue> {
    rows.iter()
        .enumerate()
        .map(|(row_idx, row)| {
            let fields = group_col_descs
                .iter()
                .enumerate()
                .filter_map(|(col_idx, desc)| {
                    let value = column_overrides
                        .get(&desc.col_name)
                        .and_then(|values| values.get(row_idx))
                        .and_then(|value| value.clone())
                        .or_else(|| {
                            group_col_indices
                                .and_then(|indices| {
                                    row.fields()
                                        .get(indices[col_idx])
                                        .filter(|field| field.name == desc.col_name)
                                        .map(|field| field.value.clone())
                                })
                                .or_else(|| row.get(&desc.col_name).cloned())
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
            if let Some(idx) = field_index {
                if let Some(field) = row.fields().get(idx).filter(|field| field.name == col_name) {
                    return Some(&field.value);
                }
            }
            row.get(col_name)
        })
        .collect()
}

fn scalar_override_columns_for_group(
    group_col_descs: &[table_control::ColumnDescContents],
    column_overrides: &HashMap<String, Vec<Option<Value>>>,
) -> Result<Option<Vec<Vec<Option<ScalarValue>>>>, StorageError> {
    let mut columns = Vec::with_capacity(group_col_descs.len());
    for desc in group_col_descs {
        if desc.is_array || desc.is_record() {
            return Ok(None);
        }
        let Some(values) = column_overrides.get(&desc.col_name) else {
            return Ok(None);
        };
        let mut scalar_values = Vec::with_capacity(values.len());
        for value in values {
            match value {
                Some(Value::Scalar(value)) => scalar_values.push(Some(value.clone())),
                Some(other) => {
                    return Err(StorageError::FormatMismatch(format!(
                        "column override {} expected scalar values, found {:?}",
                        desc.col_name,
                        other.kind()
                    )));
                }
                None => scalar_values.push(None),
            }
        }
        columns.push(scalar_values);
    }
    Ok(Some(columns))
}

/// Composite storage manager that dispatches per data manager type.
///
/// Reads `table.dat` once, then routes each DM's columns to the appropriate
/// reader (StManAipsIO or StandardStMan). Writes use StManAipsIO by default.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct CompositeStorage;

impl StorageManager for CompositeStorage {
    fn load(&self, table_path: &Path) -> Result<StorageSnapshot, StorageError> {
        self.load_with_row_hint(table_path, None)
    }

    fn save(
        &self,
        table_path: &Path,
        snapshot: &StorageSnapshot,
        dm_kind: crate::table::DataManagerKind,
        big_endian: bool,
        tile_shape: Option<&[usize]>,
    ) -> Result<(), StorageError> {
        use crate::table::DataManagerKind;

        // Clean up old data files before re-saving to prevent stale data
        // from a previous save with a different storage manager.
        if table_path.is_dir() {
            if let Ok(entries) = fs::read_dir(table_path) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy();
                    if name_str.starts_with("table.f") {
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }

        fs::create_dir_all(table_path)?;

        let schema = snapshot.schema.as_ref().ok_or_else(|| {
            StorageError::FormatMismatch("cannot save without schema".to_string())
        })?;

        let filtered_rows = filter_rows_for_save(&snapshot.rows, &snapshot.undefined_cells);
        let nrrow = filtered_rows.len() as u64;
        let data_path = table_path.join(format!("{}0", TABLE_DATA_FILE_PREFIX));
        let has_virtual = !snapshot.virtual_bindings.is_empty();

        // When saving with virtual bindings, build rows that exclude virtual columns.
        let stored_rows: Vec<RecordValue>;
        let rows_for_data = if has_virtual {
            stored_rows = snapshot
                .rows
                .iter()
                .zip(&snapshot.undefined_cells)
                .map(|(row, undef)| {
                    let stored_fields: Vec<_> = row
                        .fields()
                        .iter()
                        .filter(|f| {
                            !snapshot.virtual_columns.contains(&f.name) && !undef.contains(&f.name)
                        })
                        .cloned()
                        .collect();
                    RecordValue::new(stored_fields)
                })
                .collect();
            &stored_rows
        } else {
            &filtered_rows
        };

        let dm_type_name;
        let dm_data;
        match dm_kind {
            DataManagerKind::StManAipsIO => {
                dm_type_name = "StManAipsIO".to_string();
                dm_data = Vec::new();
                let table_dat = if has_virtual {
                    TableDatContents::from_snapshot_with_virtual(
                        schema,
                        &snapshot.keywords,
                        &snapshot.column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                        &snapshot.virtual_bindings,
                        table_path,
                    )
                } else {
                    TableDatContents::from_snapshot(
                        schema,
                        &snapshot.keywords,
                        &snapshot.column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                    )
                };
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
                // Filter col_descs to stored-only columns for the data file.
                let stored_col_descs: Vec<_> = table_dat
                    .table_desc
                    .columns
                    .iter()
                    .filter(|c| !snapshot.virtual_columns.contains(&c.col_name))
                    .cloned()
                    .collect();
                // StManAipsIO always uses canonical (big-endian) AipsIO.
                write_stman_file(
                    &data_path,
                    &stored_col_descs,
                    rows_for_data,
                    ByteOrder::BigEndian,
                )?;
            }
            DataManagerKind::StandardStMan => {
                dm_type_name = "StandardStMan".to_string();
                // Write SSM data file first (it returns the DM blob).
                // Use stored-only columns for the data file.
                let table_dat_tmp = if has_virtual {
                    TableDatContents::from_snapshot_with_virtual(
                        schema,
                        &snapshot.keywords,
                        &snapshot.column_keywords,
                        nrrow,
                        "StandardStMan",
                        &[],
                        big_endian,
                        &snapshot.virtual_bindings,
                        table_path,
                    )
                } else {
                    TableDatContents::from_snapshot(
                        schema,
                        &snapshot.keywords,
                        &snapshot.column_keywords,
                        nrrow,
                        "StandardStMan",
                        &[],
                        big_endian,
                    )
                };
                let stored_col_descs: Vec<_> = table_dat_tmp
                    .table_desc
                    .columns
                    .iter()
                    .filter(|c| !snapshot.virtual_columns.contains(&c.col_name))
                    .cloned()
                    .collect();
                dm_data = write_ssm_file(&data_path, &stored_col_descs, rows_for_data, big_endian)?;
                // Re-create table_dat with the actual DM blob.
                let table_dat = if has_virtual {
                    TableDatContents::from_snapshot_with_virtual(
                        schema,
                        &snapshot.keywords,
                        &snapshot.column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                        &snapshot.virtual_bindings,
                        table_path,
                    )
                } else {
                    TableDatContents::from_snapshot(
                        schema,
                        &snapshot.keywords,
                        &snapshot.column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                    )
                };
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
            }
            DataManagerKind::IncrementalStMan => {
                dm_type_name = "IncrementalStMan".to_string();
                // Write ISM data file first (it returns the DM blob)
                let table_dat_tmp = TableDatContents::from_snapshot(
                    schema,
                    &snapshot.keywords,
                    &snapshot.column_keywords,
                    nrrow,
                    "IncrementalStMan",
                    &[],
                    big_endian,
                );
                dm_data = write_ism_file(
                    &data_path,
                    &table_dat_tmp.table_desc.columns,
                    &snapshot.rows,
                    big_endian,
                )?;
                // Re-create table_dat with the actual DM blob
                let table_dat = TableDatContents::from_snapshot(
                    schema,
                    &snapshot.keywords,
                    &snapshot.column_keywords,
                    nrrow,
                    &dm_type_name,
                    &dm_data,
                    big_endian,
                );
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
            }
            DataManagerKind::TiledColumnStMan
            | DataManagerKind::TiledShapeStMan
            | DataManagerKind::TiledCellStMan
            | DataManagerKind::TiledDataStMan => {
                dm_type_name = match dm_kind {
                    DataManagerKind::TiledColumnStMan => "TiledColumnStMan",
                    DataManagerKind::TiledShapeStMan => "TiledShapeStMan",
                    DataManagerKind::TiledCellStMan => "TiledCellStMan",
                    DataManagerKind::TiledDataStMan => "TiledDataStMan",
                    _ => unreachable!(),
                }
                .to_string();
                dm_data = Vec::new(); // Tiled managers write empty DM blobs.
                let mut table_dat = TableDatContents::from_snapshot(
                    schema,
                    &snapshot.keywords,
                    &snapshot.column_keywords,
                    nrrow,
                    &dm_type_name,
                    &dm_data,
                    big_endian,
                );
                let dm_group_name = table_dat
                    .table_desc
                    .columns
                    .first()
                    .map(|c| c.col_name.clone())
                    .unwrap_or_default();
                for desc in &mut table_dat.table_desc.columns {
                    desc.data_manager_type = dm_type_name.clone();
                    desc.data_manager_group = dm_group_name.clone();
                }
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
                // Write tiled data to table.f0 header + table.f0_TSM* data files.
                // Use the first column name as the hypercolumn/DM name.
                let first_col_name = table_dat
                    .table_desc
                    .columns
                    .first()
                    .map(|c| c.col_name.as_str())
                    .unwrap_or("");
                tiled_stman::save_tiled_columns(
                    table_path,
                    0, // dm_seq_nr
                    &dm_type_name,
                    &table_dat.table_desc.columns,
                    &snapshot.rows,
                    big_endian,
                    tile_shape,
                    first_col_name,
                )?;
            }
        }

        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, snapshot.table_info.to_string())?;

        Ok(())
    }
}

impl CompositeStorage {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn save_borrowed(
        &self,
        table_path: &Path,
        rows: &[RecordValue],
        undefined_cells: &[HashSet<String>],
        keywords: &RecordValue,
        column_keywords: &HashMap<String, RecordValue>,
        schema: Option<&TableSchema>,
        table_info: &TableInfo,
        virtual_columns: &HashSet<String>,
        virtual_bindings: &[virtual_engine::VirtualColumnBinding],
        dm_kind: crate::table::DataManagerKind,
        big_endian: bool,
        tile_shape: Option<&[usize]>,
    ) -> Result<(), StorageError> {
        use crate::table::DataManagerKind;

        if table_path.is_dir() {
            if let Ok(entries) = fs::read_dir(table_path) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy();
                    if name_str.starts_with("table.f") {
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }

        fs::create_dir_all(table_path)?;

        let schema = schema.ok_or_else(|| {
            StorageError::FormatMismatch("cannot save without schema".to_string())
        })?;

        let filtered_rows_storage;
        let filtered_rows: &[RecordValue] = if undefined_cells.iter().any(|set| !set.is_empty()) {
            filtered_rows_storage = filter_rows_for_save(rows, undefined_cells);
            &filtered_rows_storage
        } else {
            rows
        };
        let nrrow = filtered_rows.len() as u64;
        let data_path = table_path.join(format!("{}0", TABLE_DATA_FILE_PREFIX));
        let has_virtual = !virtual_bindings.is_empty();

        let stored_rows: Vec<RecordValue>;
        let rows_for_data = if has_virtual {
            stored_rows = rows
                .iter()
                .zip(undefined_cells)
                .map(|(row, undef)| {
                    let stored_fields: Vec<_> = row
                        .fields()
                        .iter()
                        .filter(|f| !virtual_columns.contains(&f.name) && !undef.contains(&f.name))
                        .cloned()
                        .collect();
                    RecordValue::new(stored_fields)
                })
                .collect();
            &stored_rows
        } else {
            filtered_rows
        };

        let dm_type_name;
        let dm_data;
        match dm_kind {
            DataManagerKind::StManAipsIO => {
                dm_type_name = "StManAipsIO".to_string();
                dm_data = Vec::new();
                let table_dat = if has_virtual {
                    TableDatContents::from_snapshot_with_virtual(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                        virtual_bindings,
                        table_path,
                    )
                } else {
                    TableDatContents::from_snapshot(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                    )
                };
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
                let stored_col_descs: Vec<_> = table_dat
                    .table_desc
                    .columns
                    .iter()
                    .filter(|c| !virtual_columns.contains(&c.col_name))
                    .cloned()
                    .collect();
                write_stman_file(
                    &data_path,
                    &stored_col_descs,
                    rows_for_data,
                    ByteOrder::BigEndian,
                )?;
            }
            DataManagerKind::StandardStMan => {
                dm_type_name = "StandardStMan".to_string();
                let table_dat_tmp = if has_virtual {
                    TableDatContents::from_snapshot_with_virtual(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        "StandardStMan",
                        &[],
                        big_endian,
                        virtual_bindings,
                        table_path,
                    )
                } else {
                    TableDatContents::from_snapshot(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        "StandardStMan",
                        &[],
                        big_endian,
                    )
                };
                let stored_col_descs: Vec<_> = table_dat_tmp
                    .table_desc
                    .columns
                    .iter()
                    .filter(|c| !virtual_columns.contains(&c.col_name))
                    .cloned()
                    .collect();
                dm_data = write_ssm_file(&data_path, &stored_col_descs, rows_for_data, big_endian)?;
                let table_dat = if has_virtual {
                    TableDatContents::from_snapshot_with_virtual(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                        virtual_bindings,
                        table_path,
                    )
                } else {
                    TableDatContents::from_snapshot(
                        schema,
                        keywords,
                        column_keywords,
                        nrrow,
                        &dm_type_name,
                        &dm_data,
                        big_endian,
                    )
                };
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
            }
            DataManagerKind::IncrementalStMan => {
                dm_type_name = "IncrementalStMan".to_string();
                let table_dat_tmp = TableDatContents::from_snapshot(
                    schema,
                    keywords,
                    column_keywords,
                    nrrow,
                    "IncrementalStMan",
                    &[],
                    big_endian,
                );
                dm_data = write_ism_file(
                    &data_path,
                    &table_dat_tmp.table_desc.columns,
                    rows,
                    big_endian,
                )?;
                let table_dat = TableDatContents::from_snapshot(
                    schema,
                    keywords,
                    column_keywords,
                    nrrow,
                    &dm_type_name,
                    &dm_data,
                    big_endian,
                );
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
            }
            DataManagerKind::TiledColumnStMan
            | DataManagerKind::TiledShapeStMan
            | DataManagerKind::TiledCellStMan
            | DataManagerKind::TiledDataStMan => {
                dm_type_name = match dm_kind {
                    DataManagerKind::TiledColumnStMan => "TiledColumnStMan",
                    DataManagerKind::TiledShapeStMan => "TiledShapeStMan",
                    DataManagerKind::TiledCellStMan => "TiledCellStMan",
                    DataManagerKind::TiledDataStMan => "TiledDataStMan",
                    _ => unreachable!(),
                }
                .to_string();
                dm_data = Vec::new();
                let mut table_dat = TableDatContents::from_snapshot(
                    schema,
                    keywords,
                    column_keywords,
                    nrrow,
                    &dm_type_name,
                    &dm_data,
                    big_endian,
                );
                let dm_group_name = table_dat
                    .table_desc
                    .columns
                    .first()
                    .map(|c| c.col_name.clone())
                    .unwrap_or_default();
                for desc in &mut table_dat.table_desc.columns {
                    desc.data_manager_type = dm_type_name.clone();
                    desc.data_manager_group = dm_group_name.clone();
                }
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
                let first_col_name = table_dat
                    .table_desc
                    .columns
                    .first()
                    .map(|c| c.col_name.as_str())
                    .unwrap_or("");
                tiled_stman::save_tiled_columns(
                    table_path,
                    0,
                    &dm_type_name,
                    &table_dat.table_desc.columns,
                    rows,
                    big_endian,
                    tile_shape,
                    first_col_name,
                )?;
            }
        }

        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, table_info.to_string())?;
        Ok(())
    }

    pub(crate) fn load_with_row_hint(
        &self,
        table_path: &Path,
        row_hint: Option<u64>,
    ) -> Result<StorageSnapshot, StorageError> {
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !control_path.is_file() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => {
                self.load_plain_table_filtered(table_path, &table_dat, row_hint, None)
            }
            TableDatResult::Ref(ref_dat) => self.load_ref_table(table_path, &ref_dat),
            TableDatResult::Concat(concat_dat) => self.load_concat_table(table_path, &concat_dat),
        }
    }

    pub(crate) fn load_selected_columns_with_row_hint(
        &self,
        table_path: &Path,
        columns: &[&str],
        row_hint: Option<u64>,
    ) -> Result<StorageSnapshot, StorageError> {
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !control_path.is_file() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        let requested_columns: HashSet<&str> = columns.iter().copied().collect();
        let mut snapshot = match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => self.load_plain_table_filtered(
                table_path,
                &table_dat,
                row_hint,
                Some(&requested_columns),
            ),
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let mut snapshot = self.load_with_row_hint(table_path, row_hint)?;
                for row in &mut snapshot.rows {
                    *row = RecordValue::new(
                        row.fields()
                            .iter()
                            .filter(|field| requested_columns.contains(field.name.as_str()))
                            .cloned()
                            .collect(),
                    );
                }
                for undefined in &mut snapshot.undefined_cells {
                    undefined.retain(|column| requested_columns.contains(column.as_str()));
                }
                Ok(snapshot)
            }
        }?;
        for row in &mut snapshot.rows {
            *row = reorder_row_to_requested_columns(row, columns);
        }
        Ok(snapshot)
    }

    pub(crate) fn save_metadata_only(
        &self,
        table_path: &Path,
        snapshot: &StorageSnapshot,
    ) -> Result<(), StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(mut table_dat) => {
                table_dat.table_desc.table_keywords = snapshot.keywords.clone();
                for column in &mut table_dat.table_desc.columns {
                    column.keywords = snapshot
                        .column_keywords
                        .get(&column.col_name)
                        .cloned()
                        .unwrap_or_default();
                }
                write_table_dat(&control_path, &table_dat)?;
                let info_path = table_path.join(TABLE_INFO_FILE);
                fs::write(&info_path, snapshot.table_info.to_string())?;
                Ok(())
            }
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                Err(StorageError::FormatMismatch(
                    "metadata-only save is only supported for PlainTable storage".to_string(),
                ))
            }
        }
    }

    pub(crate) fn load_metadata_only(
        &self,
        table_path: &Path,
    ) -> Result<StorageSnapshot, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => {
                self.load_plain_table_metadata(table_path, &table_dat)
            }
            // Metadata-only open is primarily for plain tiled tables. Fall back
            // to the full loader for more complex table types.
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let mut snapshot = self.load(table_path)?;
                snapshot.rows.clear();
                Ok(snapshot)
            }
        }
    }

    pub(crate) fn load_scalar_columns_with_row_hint(
        &self,
        table_path: &Path,
        row_hint: Option<u64>,
    ) -> Result<ScalarColumnSnapshot, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => {
                self.load_plain_scalar_columns(table_path, &table_dat, row_hint)
            }
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let snapshot = self.load_with_row_hint(table_path, row_hint)?;
                Ok(scalar_columns_from_snapshot(&snapshot))
            }
        }
    }

    pub(crate) fn load_scalar_column_with_row_hint(
        &self,
        table_path: &Path,
        column: &str,
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ScalarValue>>, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => {
                self.load_plain_scalar_column(table_path, &table_dat, column, row_hint)
            }
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let snapshot = self.load_with_row_hint(table_path, row_hint)?;
                scalar_column_from_snapshot(&snapshot, column)
            }
        }
    }

    pub(crate) fn load_scalar_column_rows_with_row_hint(
        &self,
        table_path: &Path,
        column: &str,
        selected_rows: &[usize],
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ScalarValue>>, StorageError> {
        if selected_rows.is_empty() {
            return Ok(Vec::new());
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => self.load_plain_scalar_column_rows(
                table_path,
                &table_dat,
                column,
                selected_rows,
                row_hint,
            ),
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let snapshot = self.load_with_row_hint(table_path, row_hint)?;
                let values = scalar_column_from_snapshot(&snapshot, column)?;
                Ok(select_scalar_rows(&values, selected_rows))
            }
        }
    }

    pub(crate) fn load_array_column_with_row_hint(
        &self,
        table_path: &Path,
        column: &str,
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => {
                self.load_plain_array_column(table_path, &table_dat, column, row_hint)
            }
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let snapshot = self.load_with_row_hint(table_path, row_hint)?;
                array_column_from_snapshot(&snapshot, column)
            }
        }
    }

    pub(crate) fn load_array_column_rows_with_row_hint(
        &self,
        table_path: &Path,
        column: &str,
        selected_rows: &[usize],
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        if selected_rows.is_empty() {
            return Ok(Vec::new());
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => self.load_plain_array_column_rows(
                table_path,
                &table_dat,
                column,
                selected_rows,
                row_hint,
            ),
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let snapshot = self.load_with_row_hint(table_path, row_hint)?;
                let values = array_column_from_snapshot(&snapshot, column)?;
                Ok(select_array_rows(&values, selected_rows))
            }
        }
    }

    pub(crate) fn load_array_column_rows_2d_channel_range_with_row_hint(
        &self,
        table_path: &Path,
        column: &str,
        selected_rows: &[usize],
        channel_start: usize,
        channel_count: usize,
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        if selected_rows.is_empty() {
            return Ok(Vec::new());
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }
        if !control_path.exists() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => self.load_plain_array_column_rows_2d_channel_range(
                table_path,
                &table_dat,
                column,
                selected_rows,
                channel_start,
                channel_count,
                row_hint,
            ),
            TableDatResult::Ref(_) | TableDatResult::Concat(_) => {
                let snapshot = self.load_with_row_hint(table_path, row_hint)?;
                let values = array_column_from_snapshot(&snapshot, column)?;
                select_array_rows(&values, selected_rows)
                    .into_iter()
                    .map(|value| {
                        value
                            .map(|array| {
                                slice_array_value_2d_channel_range(
                                    array,
                                    channel_start,
                                    channel_count,
                                )
                            })
                            .transpose()
                    })
                    .collect()
            }
        }
    }

    /// Load a PlainTable from table.dat contents and data files.
    ///
    /// Uses two-pass loading:
    /// - Pass 1: Load stored columns from storage managers (StManAipsIO, StandardStMan).
    /// - Pass 2: Materialize virtual columns from virtual engines.
    /// - Pass 3: Reject any remaining unknown DM types.
    fn load_plain_table_filtered(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        row_hint: Option<u64>,
        requested_columns: Option<&HashSet<&str>>,
    ) -> Result<StorageSnapshot, StorageError> {
        if let Some(requested_columns) = requested_columns
            && table_dat
                .column_set
                .data_managers
                .iter()
                .any(|dm| is_virtual_engine(&dm.type_name))
        {
            let mut snapshot =
                self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            for row in &mut snapshot.rows {
                *row = RecordValue::new(
                    row.fields()
                        .iter()
                        .filter(|field| requested_columns.contains(field.name.as_str()))
                        .cloned()
                        .collect(),
                );
            }
            for undefined in &mut snapshot.undefined_cells {
                undefined.retain(|column| requested_columns.contains(column.as_str()));
            }
            return Ok(snapshot);
        }

        let schema = table_dat.to_table_schema()?;
        let keywords = table_dat.table_desc.table_keywords.clone();
        let column_keywords: HashMap<String, RecordValue> = table_dat
            .table_desc
            .columns
            .iter()
            .filter(|c| {
                requested_columns.is_none_or(|requested| requested.contains(c.col_name.as_str()))
            })
            .filter(|c| !c.keywords.fields().is_empty())
            .map(|c| (c.col_name.clone(), c.keywords.clone()))
            .collect();
        // Real-world casacore tables can carry a stale or zero row count in
        // the outer "Table" envelope while the nested ColumnSet/storage-
        // manager state still reflects the actual number of rows. Upstream
        // C++ lets the ColumnSet/data-manager layer override the caller's
        // row count during open, so use the larger of the two counts here.
        let nrrow = table_dat
            .nrrow
            .max(table_dat.column_set.nrrow)
            .max(row_hint.unwrap_or(0)) as usize;

        let mut rows: Vec<RecordValue> = (0..nrrow).map(|_| RecordValue::default()).collect();
        let mut undefined_cells: Vec<HashSet<String>> =
            (0..nrrow).map(|_| HashSet::new()).collect();
        let mut virtual_columns = HashSet::new();

        // Pass 1: Load stored columns from storage managers.
        for dm in &table_dat.column_set.data_managers {
            if is_virtual_engine(&dm.type_name) {
                continue; // Handled in pass 2.
            }

            let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));

            let all_bound_cols: Vec<(usize, &_)> = table_dat
                .column_set
                .columns
                .iter()
                .enumerate()
                .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
                .collect();
            let bound_cols: Vec<(usize, &_)> = all_bound_cols
                .iter()
                .copied()
                .filter(|(_, pc)| {
                    requested_columns
                        .is_none_or(|requested| requested.contains(pc.original_name.as_str()))
                })
                .collect();

            if bound_cols.is_empty() {
                continue;
            }

            match dm.type_name.as_str() {
                "StManAipsIO" => {
                    if !data_path.is_file() {
                        return Err(StorageError::MissingDataFile(data_path));
                    }
                    load_stman_aipsio_columns(
                        &data_path,
                        &table_dat.table_desc.columns,
                        &all_bound_cols,
                        &mut rows,
                        &mut undefined_cells,
                        nrrow,
                        ByteOrder::BigEndian,
                    )
                    .map_err(|err| {
                        StorageError::FormatMismatch(format!(
                            "while loading {} seq {} from {}: {err}",
                            dm.type_name,
                            dm.seq_nr,
                            data_path.display()
                        ))
                    })?;
                }
                "StandardStMan" => {
                    if !data_path.is_file() {
                        return Err(StorageError::MissingDataFile(data_path));
                    }
                    load_ssm_columns(
                        &data_path,
                        &dm.data,
                        &table_dat.table_desc.columns,
                        &bound_cols,
                        &mut rows,
                        &mut undefined_cells,
                        nrrow,
                    )
                    .map_err(|err| {
                        StorageError::FormatMismatch(format!(
                            "while loading {} seq {} from {}: {err}",
                            dm.type_name,
                            dm.seq_nr,
                            data_path.display()
                        ))
                    })?;
                }
                "IncrementalStMan" => {
                    if !data_path.is_file() {
                        return Err(StorageError::MissingDataFile(data_path));
                    }
                    load_ism_columns(
                        &data_path,
                        &dm.data,
                        &table_dat.table_desc.columns,
                        &bound_cols,
                        &mut rows,
                        &mut undefined_cells,
                        nrrow,
                    )
                    .map_err(|err| {
                        StorageError::FormatMismatch(format!(
                            "while loading {} seq {} from {}: {err}",
                            dm.type_name,
                            dm.seq_nr,
                            data_path.display()
                        ))
                    })?;
                }
                "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                    tiled_stman::load_tiled_columns(
                        table_path,
                        dm,
                        &table_dat.table_desc.columns,
                        &bound_cols,
                        &mut rows,
                        &mut undefined_cells,
                        nrrow,
                    )
                    .map_err(|err| {
                        StorageError::FormatMismatch(format!(
                            "while loading {} seq {} from {}: {err}",
                            dm.type_name,
                            dm.seq_nr,
                            data_path.display()
                        ))
                    })?;
                }
                other => {
                    return Err(StorageError::UnsupportedDataManager(other.to_string()));
                }
            }
        }

        // Pass 2: Materialize virtual columns.
        // Only clone rows if there are virtual engines that need the stored
        // snapshot — avoids an O(rows × cols) clone for tables with no
        // virtual columns (the common case for MS subtables).
        let has_virtual = table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name));

        if has_virtual {
            let stored_rows = rows.clone();
            for dm in &table_dat.column_set.data_managers {
                if !is_virtual_engine(&dm.type_name) {
                    continue;
                }

                let engine = lookup_engine(&dm.type_name)
                    .ok_or_else(|| StorageError::UnsupportedDataManager(dm.type_name.clone()))?;

                let bound_cols: Vec<(usize, &_)> = table_dat
                    .column_set
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
                    .filter(|(_, pc)| {
                        requested_columns
                            .is_none_or(|requested| requested.contains(pc.original_name.as_str()))
                    })
                    .collect();

                if bound_cols.is_empty() {
                    continue;
                }

                for &(desc_idx, _) in &bound_cols {
                    virtual_columns.insert(table_dat.table_desc.columns[desc_idx].col_name.clone());
                }

                let ctx = VirtualContext {
                    col_descs: &table_dat.table_desc.columns,
                    rows: &stored_rows,
                    table_path,
                    nrrow,
                };

                engine.materialize(&ctx, &bound_cols, &mut rows)?;
            }
        }

        let table_info = load_table_info(table_path);

        // Build DM info from table.dat entries.
        let dm_info = extract_dm_info(table_dat);

        if let Some(requested_columns) = requested_columns {
            for row in &mut rows {
                *row = RecordValue::new(
                    row.fields()
                        .iter()
                        .filter(|field| requested_columns.contains(field.name.as_str()))
                        .cloned()
                        .collect(),
                );
            }
            for undefined in &mut undefined_cells {
                undefined.retain(|column| requested_columns.contains(column.as_str()));
            }
        }

        Ok(StorageSnapshot {
            row_count: rows.len(),
            rows,
            undefined_cells,
            keywords,
            column_keywords,
            schema: Some(schema),
            table_info,
            virtual_columns,
            virtual_bindings: Vec::new(),
            dm_info,
        })
    }

    fn load_plain_scalar_columns(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        row_hint: Option<u64>,
    ) -> Result<ScalarColumnSnapshot, StorageError> {
        self.load_plain_scalar_columns_filtered(table_path, table_dat, row_hint, None)
    }

    fn load_plain_scalar_column(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ScalarValue>>, StorageError> {
        if table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name))
        {
            let snapshot = self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            return scalar_column_from_snapshot(&snapshot, column);
        }

        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("scalar column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if col_desc.is_array || col_desc.is_record() {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not a scalar column"
            )));
        }

        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "scalar column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "scalar column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
        let nrrow = table_dat
            .nrrow
            .max(table_dat.column_set.nrrow)
            .max(row_hint.unwrap_or(0)) as usize;
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();
        let target_col_idx = bound_cols
            .iter()
            .position(|(candidate_desc_idx, _)| *candidate_desc_idx == desc_idx)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "scalar column '{column}' missing data-manager binding index"
                ))
            })?;

        if dm.type_name == "StManAipsIO" {
            if !data_path.is_file() {
                return Err(StorageError::MissingDataFile(data_path));
            }
            let dm_columns: Vec<_> = bound_cols
                .iter()
                .map(|(candidate_desc_idx, _)| {
                    table_dat.table_desc.columns[*candidate_desc_idx].clone()
                })
                .collect();
            if let Some(values) = read_stman_scalar_column(
                &data_path,
                &dm_columns,
                target_col_idx,
                ByteOrder::BigEndian,
            )? {
                return Ok(values);
            }
        }
        if dm.type_name == "IncrementalStMan" {
            if !data_path.is_file() {
                return Err(StorageError::MissingDataFile(data_path));
            }
            let group_col_descs: Vec<_> = bound_cols
                .iter()
                .map(|(bound_desc_idx, _)| &table_dat.table_desc.columns[*bound_desc_idx])
                .collect();
            if let Some(values) = read_ism_scalar_column(
                &data_path,
                &dm.data,
                &group_col_descs,
                target_col_idx,
                nrrow,
            )? {
                return Ok(values);
            }
        }

        let snapshot = self.load_plain_scalar_columns(table_path, table_dat, row_hint)?;
        snapshot.columns.get(column).cloned().ok_or_else(|| {
            StorageError::FormatMismatch(format!("scalar column '{column}' not found"))
        })
    }

    fn load_plain_scalar_columns_filtered(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        row_hint: Option<u64>,
        requested_columns: Option<&HashSet<&str>>,
    ) -> Result<ScalarColumnSnapshot, StorageError> {
        let nrrow = table_dat
            .nrrow
            .max(table_dat.column_set.nrrow)
            .max(row_hint.unwrap_or(0)) as usize;

        if table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name))
        {
            let snapshot = self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            return Ok(scalar_columns_from_snapshot(&snapshot));
        }

        let mut columns = HashMap::new();

        for dm in &table_dat.column_set.data_managers {
            let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
            let all_bound_cols: Vec<(usize, &_)> = table_dat
                .column_set
                .columns
                .iter()
                .enumerate()
                .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
                .collect();
            if all_bound_cols.is_empty() {
                continue;
            }
            let bound_cols: Vec<(usize, &_)> = all_bound_cols
                .iter()
                .copied()
                .filter(|(_, pc)| {
                    requested_columns
                        .is_none_or(|requested| requested.contains(pc.original_name.as_str()))
                })
                .collect();
            if bound_cols.is_empty() {
                continue;
            }

            match dm.type_name.as_str() {
                "StManAipsIO" => {
                    if !data_path.is_file() {
                        return Err(StorageError::MissingDataFile(data_path));
                    }
                    collect_stman_scalar_columns(
                        &data_path,
                        &table_dat.table_desc.columns,
                        &all_bound_cols,
                        requested_columns,
                        nrrow,
                        ByteOrder::BigEndian,
                        &mut columns,
                    )?;
                }
                "StandardStMan" => {
                    if !data_path.is_file() {
                        return Err(StorageError::MissingDataFile(data_path));
                    }
                    collect_ssm_scalar_columns(
                        &data_path,
                        &dm.data,
                        &table_dat.table_desc.columns,
                        &bound_cols,
                        nrrow,
                        &mut columns,
                    )?;
                }
                "IncrementalStMan" => {
                    if !data_path.is_file() {
                        return Err(StorageError::MissingDataFile(data_path));
                    }
                    collect_ism_scalar_columns(
                        &data_path,
                        &dm.data,
                        &table_dat.table_desc.columns,
                        &bound_cols,
                        nrrow,
                        &mut columns,
                    )?;
                }
                "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                    if bound_cols
                        .iter()
                        .any(|(desc_idx, _)| !table_dat.table_desc.columns[*desc_idx].is_array)
                    {
                        let snapshot =
                            self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
                        return Ok(scalar_columns_from_snapshot(&snapshot));
                    }
                }
                other => {
                    return Err(StorageError::UnsupportedDataManager(other.to_string()));
                }
            }
        }

        Ok(ScalarColumnSnapshot {
            row_count: nrrow,
            columns,
        })
    }

    fn load_plain_scalar_column_rows(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        selected_rows: &[usize],
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ScalarValue>>, StorageError> {
        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("scalar column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if col_desc.is_array || col_desc.is_record() {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not a scalar column"
            )));
        }

        if table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name))
        {
            let snapshot = self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            let values = scalar_column_from_snapshot(&snapshot, column)?;
            return Ok(select_scalar_rows(&values, selected_rows));
        }

        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "scalar column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "scalar column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();
        let target_col_idx = bound_cols
            .iter()
            .position(|(candidate_desc_idx, _)| *candidate_desc_idx == desc_idx)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "scalar column '{column}' missing data-manager binding index"
                ))
            })?;

        match dm.type_name.as_str() {
            "StManAipsIO" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| {
                        table_dat.table_desc.columns[*bound_desc_idx].clone()
                    })
                    .collect();
                if let Some(values) = read_stman_scalar_column_rows(
                    &data_path,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                    ByteOrder::BigEndian,
                )? {
                    return Ok(values);
                }
            }
            "StandardStMan" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| &table_dat.table_desc.columns[*bound_desc_idx])
                    .collect();
                if let Some(values) = read_ssm_scalar_column_rows(
                    &data_path,
                    &dm.data,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                )? {
                    return Ok(values);
                }
            }
            "IncrementalStMan" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| &table_dat.table_desc.columns[*bound_desc_idx])
                    .collect();
                if let Some(values) = read_ism_scalar_column_rows(
                    &data_path,
                    &dm.data,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                )? {
                    return Ok(values);
                }
            }
            _ => {}
        }

        let values = self.load_plain_scalar_column(table_path, table_dat, column, row_hint)?;
        Ok(select_scalar_rows(&values, selected_rows))
    }

    fn load_plain_array_column(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("array column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if !col_desc.is_array {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not an array column"
            )));
        }

        if table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name))
        {
            let snapshot = self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            return array_column_from_snapshot(&snapshot, column);
        }

        let nrrow = table_dat
            .nrrow
            .max(table_dat.column_set.nrrow)
            .max(row_hint.unwrap_or(0)) as usize;
        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();
        let mut rows: Vec<RecordValue> = (0..nrrow).map(|_| RecordValue::default()).collect();
        let mut undefined_cells: Vec<HashSet<String>> =
            (0..nrrow).map(|_| HashSet::new()).collect();

        match dm.type_name.as_str() {
            "StManAipsIO" => {
                if !data_path.is_file() {
                    return Err(StorageError::MissingDataFile(data_path));
                }
                load_stman_aipsio_columns(
                    &data_path,
                    &table_dat.table_desc.columns,
                    &bound_cols,
                    &mut rows,
                    &mut undefined_cells,
                    nrrow,
                    ByteOrder::BigEndian,
                )?;
            }
            "StandardStMan" => {
                if !data_path.is_file() {
                    return Err(StorageError::MissingDataFile(data_path));
                }
                load_ssm_columns(
                    &data_path,
                    &dm.data,
                    &table_dat.table_desc.columns,
                    &bound_cols,
                    &mut rows,
                    &mut undefined_cells,
                    nrrow,
                )?;
            }
            "IncrementalStMan" => {
                if !data_path.is_file() {
                    return Err(StorageError::MissingDataFile(data_path));
                }
                load_ism_columns(
                    &data_path,
                    &dm.data,
                    &table_dat.table_desc.columns,
                    &bound_cols,
                    &mut rows,
                    &mut undefined_cells,
                    nrrow,
                )?;
            }
            "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                tiled_stman::load_tiled_columns(
                    table_path,
                    dm,
                    &table_dat.table_desc.columns,
                    &bound_cols,
                    &mut rows,
                    &mut undefined_cells,
                    nrrow,
                )?;
            }
            other => {
                return Err(StorageError::UnsupportedDataManager(other.to_string()));
            }
        }

        let snapshot = StorageSnapshot {
            row_count: nrrow,
            rows,
            undefined_cells,
            keywords: RecordValue::default(),
            column_keywords: HashMap::new(),
            schema: None,
            table_info: TableInfo::default(),
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            dm_info: Vec::new(),
        };
        array_column_from_snapshot(&snapshot, column)
    }

    fn load_plain_array_column_rows(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        selected_rows: &[usize],
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("array column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if !col_desc.is_array {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not an array column"
            )));
        }

        if table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name))
        {
            let snapshot = self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            let values = array_column_from_snapshot(&snapshot, column)?;
            return Ok(select_array_rows(&values, selected_rows));
        }

        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();
        let target_col_idx = bound_cols
            .iter()
            .position(|(bound_desc_idx, _)| *bound_desc_idx == desc_idx)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data-manager column binding"
                ))
            })?;

        match dm.type_name.as_str() {
            "StManAipsIO" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| {
                        table_dat.table_desc.columns[*bound_desc_idx].clone()
                    })
                    .collect();
                if let Some(values) = read_stman_array_column_rows(
                    &data_path,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                    ByteOrder::BigEndian,
                )? {
                    return Ok(values);
                }
                let values =
                    self.load_plain_array_column(table_path, table_dat, column, row_hint)?;
                Ok(select_array_rows(&values, selected_rows))
            }
            "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                tiled_stman::load_tiled_column_rows(
                    table_path,
                    dm,
                    &table_dat.table_desc.columns,
                    &bound_cols,
                    desc_idx,
                    selected_rows,
                )
            }
            "StandardStMan" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| &table_dat.table_desc.columns[*bound_desc_idx])
                    .collect();
                if let Some(values) = read_ssm_array_column_rows(
                    &data_path,
                    &dm.data,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                )? {
                    return Ok(values);
                }
                let values =
                    self.load_plain_array_column(table_path, table_dat, column, row_hint)?;
                Ok(select_array_rows(&values, selected_rows))
            }
            _ => {
                let values =
                    self.load_plain_array_column(table_path, table_dat, column, row_hint)?;
                Ok(select_array_rows(&values, selected_rows))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn load_plain_array_column_rows_2d_channel_range(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
        column: &str,
        selected_rows: &[usize],
        channel_start: usize,
        channel_count: usize,
        row_hint: Option<u64>,
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        let desc_idx = table_dat
            .table_desc
            .columns
            .iter()
            .position(|desc| desc.col_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("array column '{column}' not found"))
            })?;
        let col_desc = &table_dat.table_desc.columns[desc_idx];
        if !col_desc.is_array {
            return Err(StorageError::FormatMismatch(format!(
                "column '{column}' is not an array column"
            )));
        }

        if table_dat
            .column_set
            .data_managers
            .iter()
            .any(|dm| is_virtual_engine(&dm.type_name))
        {
            let snapshot = self.load_plain_table_filtered(table_path, table_dat, row_hint, None)?;
            let values = array_column_from_snapshot(&snapshot, column)?;
            return select_array_rows(&values, selected_rows)
                .into_iter()
                .map(|value| {
                    value
                        .map(|array| {
                            slice_array_value_2d_channel_range(array, channel_start, channel_count)
                        })
                        .transpose()
                })
                .collect();
        }

        let dm_seq_nr = table_dat
            .column_set
            .columns
            .iter()
            .find(|entry| entry.original_name == column)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let dm = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|dm| dm.seq_nr == dm_seq_nr)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data manager {dm_seq_nr}"
                ))
            })?;
        let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));
        let bound_cols: Vec<(usize, &_)> = table_dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
            .collect();
        let target_col_idx = bound_cols
            .iter()
            .position(|(bound_desc_idx, _)| *bound_desc_idx == desc_idx)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{column}' missing data-manager column binding"
                ))
            })?;

        let values = match dm.type_name.as_str() {
            "TiledShapeStMan" => {
                return tiled_stman::load_tiled_column_rows_2d_channel_range(
                    table_path,
                    dm,
                    &table_dat.table_desc.columns,
                    &bound_cols,
                    desc_idx,
                    selected_rows,
                    channel_start,
                    channel_count,
                );
            }
            "StManAipsIO" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| {
                        table_dat.table_desc.columns[*bound_desc_idx].clone()
                    })
                    .collect();
                if let Some(values) = read_stman_array_column_rows(
                    &data_path,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                    ByteOrder::BigEndian,
                )? {
                    values
                } else {
                    let values =
                        self.load_plain_array_column(table_path, table_dat, column, row_hint)?;
                    select_array_rows(&values, selected_rows)
                }
            }
            "StandardStMan" => {
                let group_col_descs: Vec<_> = bound_cols
                    .iter()
                    .map(|(bound_desc_idx, _)| &table_dat.table_desc.columns[*bound_desc_idx])
                    .collect();
                if let Some(values) = read_ssm_array_column_rows(
                    &data_path,
                    &dm.data,
                    &group_col_descs,
                    target_col_idx,
                    selected_rows,
                )? {
                    values
                } else {
                    let values =
                        self.load_plain_array_column(table_path, table_dat, column, row_hint)?;
                    select_array_rows(&values, selected_rows)
                }
            }
            _ => {
                let values =
                    self.load_plain_array_column(table_path, table_dat, column, row_hint)?;
                select_array_rows(&values, selected_rows)
            }
        };

        values
            .into_iter()
            .map(|value| {
                value
                    .map(|array| {
                        slice_array_value_2d_channel_range(array, channel_start, channel_count)
                    })
                    .transpose()
            })
            .collect()
    }

    fn load_plain_table_metadata(
        &self,
        table_path: &Path,
        table_dat: &TableDatContents,
    ) -> Result<StorageSnapshot, StorageError> {
        let schema = table_dat.to_table_schema()?;
        let keywords = table_dat.table_desc.table_keywords.clone();
        let column_keywords: HashMap<String, RecordValue> = table_dat
            .table_desc
            .columns
            .iter()
            .filter(|c| !c.keywords.fields().is_empty())
            .map(|c| (c.col_name.clone(), c.keywords.clone()))
            .collect();
        let mut virtual_columns = HashSet::new();
        for dm in &table_dat.column_set.data_managers {
            if !is_virtual_engine(&dm.type_name) {
                continue;
            }
            for plain_col in &table_dat.column_set.columns {
                if plain_col.dm_seq_nr != dm.seq_nr {
                    continue;
                }
                virtual_columns.insert(plain_col.original_name.clone());
            }
        }

        Ok(StorageSnapshot {
            row_count: table_dat.nrrow.max(table_dat.column_set.nrrow) as usize,
            rows: Vec::new(),
            undefined_cells: Vec::new(),
            keywords,
            column_keywords,
            schema: Some(schema),
            table_info: load_table_info(table_path),
            virtual_columns,
            virtual_bindings: Vec::new(),
            dm_info: extract_dm_info(table_dat),
        })
    }

    /// Load a RefTable by opening the parent and extracting referenced rows.
    fn load_ref_table(
        &self,
        table_path: &Path,
        ref_dat: &RefTableDatContents,
    ) -> Result<StorageSnapshot, StorageError> {
        // Resolve parent path using C++ addDirectory convention:
        //  - Leading "./" means: take parent directory of ref table, then append
        //  - Leading "././" means: the path is inside the ref table directory
        //  - Otherwise: use as-is (absolute or plain relative)
        let parent_path = add_directory(&ref_dat.parent_relative_path, table_path)?;

        // Recursively load the parent table.
        let parent = self.load(&parent_path)?;

        // Validate that the parent hasn't shrunk.
        if ref_dat.parent_nrrow > parent.rows.len() as u64 {
            return Err(StorageError::FormatMismatch(format!(
                "parent table has {} rows but RefTable expects at least {}",
                parent.rows.len(),
                ref_dat.parent_nrrow
            )));
        }

        // Extract referenced rows.
        let mut rows = Vec::with_capacity(ref_dat.row_map.len());
        let mut undefined_cells = Vec::with_capacity(ref_dat.row_map.len());
        for &parent_row in &ref_dat.row_map {
            let idx = parent_row as usize;
            if idx >= parent.rows.len() {
                return Err(StorageError::FormatMismatch(format!(
                    "RefTable references parent row {idx} but parent has {} rows",
                    parent.rows.len()
                )));
            }
            // Apply column projection if the view uses a subset of columns.
            if ref_dat.column_names.len() < parent.rows[idx].fields().len() {
                let mut projected = RecordValue::default();
                let mut projected_undefined = HashSet::new();
                for view_col in &ref_dat.column_names {
                    // Resolve view column name to parent column name.
                    let parent_col = ref_dat
                        .column_name_map
                        .iter()
                        .find(|(v, _)| v == view_col)
                        .map(|(_, p)| p.as_str())
                        .unwrap_or(view_col.as_str());
                    if let Some(val) = parent.rows[idx].get(parent_col) {
                        projected.push(RecordField::new(view_col.clone(), val.clone()));
                    }
                    if parent.undefined_cells[idx].contains(parent_col) {
                        projected_undefined.insert(view_col.clone());
                    }
                }
                rows.push(projected);
                undefined_cells.push(projected_undefined);
            } else {
                rows.push(parent.rows[idx].clone());
                undefined_cells.push(parent.undefined_cells[idx].clone());
            }
        }

        // Build projected schema if applicable.
        let schema = parent.schema.and_then(|s| {
            if ref_dat.column_names.len() < s.columns().len() {
                let cols: Vec<_> = ref_dat
                    .column_names
                    .iter()
                    .filter_map(|name| {
                        let parent_name = ref_dat
                            .column_name_map
                            .iter()
                            .find(|(v, _)| v == name)
                            .map(|(_, p)| p.as_str())
                            .unwrap_or(name.as_str());
                        s.columns()
                            .iter()
                            .find(|c| c.name() == parent_name)
                            .cloned()
                    })
                    .collect();
                crate::schema::TableSchema::new(cols).ok()
            } else {
                Some(s)
            }
        });

        let table_info = load_table_info(table_path);

        Ok(StorageSnapshot {
            row_count: rows.len(),
            rows,
            undefined_cells,
            keywords: parent.keywords,
            column_keywords: parent.column_keywords,
            schema,
            table_info,
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            dm_info: vec![],
        })
    }

    /// Save a RefTable to disk (table.dat only, no data files).
    pub(crate) fn save_ref_table(
        &self,
        table_path: &Path,
        contents: &RefTableDatContents,
        table_info: &TableInfo,
    ) -> Result<(), StorageError> {
        fs::create_dir_all(table_path)?;
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        write_ref_table_dat(&control_path, contents)?;
        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, table_info.to_string())?;
        Ok(())
    }

    /// Load a ConcatTable by opening all constituent tables and collecting rows.
    ///
    /// Materializes the concatenation: all rows from all constituent tables
    /// are collected into a single `StorageSnapshot`. Keywords and schema
    /// come from the first constituent table.
    fn load_concat_table(
        &self,
        table_path: &Path,
        concat_dat: &table_control::ConcatTableDatContents,
    ) -> Result<StorageSnapshot, StorageError> {
        let mut all_rows = Vec::new();
        let mut schema = None;
        let mut keywords = RecordValue::default();
        let mut column_keywords = HashMap::new();
        let mut undefined_cells = Vec::new();

        for (i, rel_path) in concat_dat.table_paths.iter().enumerate() {
            let abs_path = add_directory(rel_path, table_path)?;
            let sub_snapshot = self.load(&abs_path)?;

            if i == 0 {
                schema = sub_snapshot.schema;
                keywords = sub_snapshot.keywords;
                column_keywords = sub_snapshot.column_keywords;
            }
            all_rows.extend(sub_snapshot.rows);
            undefined_cells.extend(sub_snapshot.undefined_cells);
        }

        let table_info = load_table_info(table_path);

        Ok(StorageSnapshot {
            row_count: all_rows.len(),
            rows: all_rows,
            undefined_cells,
            keywords,
            column_keywords,
            schema,
            table_info,
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            dm_info: vec![],
        })
    }

    /// Save a ConcatTable to disk (table.dat + table.info only, no data files).
    pub(crate) fn save_concat_table(
        &self,
        table_path: &Path,
        contents: &table_control::ConcatTableDatContents,
        table_info: &TableInfo,
    ) -> Result<(), StorageError> {
        fs::create_dir_all(table_path)?;
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        write_concat_table_dat(&control_path, contents)?;
        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, table_info.to_string())?;
        Ok(())
    }

    /// Save with per-column DM bindings.
    ///
    /// Columns listed in `bindings` use their specified DM; all other columns
    /// use the default DM from `dm_kind`. Each DM group writes its own data
    /// file (`table.f0`, `table.f1`, ...) and gets a separate entry in `table.dat`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn save_with_bindings_borrowed(
        &self,
        table_path: &Path,
        rows: &[RecordValue],
        undefined_cells: &[HashSet<String>],
        keywords: &RecordValue,
        column_keywords: &HashMap<String, RecordValue>,
        schema: Option<&TableSchema>,
        table_info: &TableInfo,
        virtual_columns: &HashSet<String>,
        virtual_bindings: &[virtual_engine::VirtualColumnBinding],
        default_dm: crate::table::DataManagerKind,
        big_endian: bool,
        default_tile_shape: Option<&[usize]>,
        bindings: &std::collections::HashMap<String, crate::table::ColumnBinding>,
    ) -> Result<(), StorageError> {
        let column_overrides = HashMap::new();
        self.save_with_bindings_and_column_overrides_borrowed(
            table_path,
            rows,
            undefined_cells,
            keywords,
            column_keywords,
            schema,
            table_info,
            virtual_columns,
            virtual_bindings,
            default_dm,
            big_endian,
            default_tile_shape,
            bindings,
            &column_overrides,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn save_with_bindings_and_column_overrides_borrowed(
        &self,
        table_path: &Path,
        rows: &[RecordValue],
        undefined_cells: &[HashSet<String>],
        keywords: &RecordValue,
        column_keywords: &HashMap<String, RecordValue>,
        schema: Option<&TableSchema>,
        table_info: &TableInfo,
        virtual_columns: &HashSet<String>,
        virtual_bindings: &[virtual_engine::VirtualColumnBinding],
        default_dm: crate::table::DataManagerKind,
        big_endian: bool,
        default_tile_shape: Option<&[usize]>,
        bindings: &std::collections::HashMap<String, crate::table::ColumnBinding>,
        column_overrides: &HashMap<String, Vec<Option<Value>>>,
    ) -> Result<(), StorageError> {
        use crate::table::DataManagerKind;

        let mut profiler = StorageProfiler::start(format!(
            "CompositeStorage::save_with_bindings path={}",
            table_path.display()
        ));

        fs::create_dir_all(table_path)?;

        let schema = schema.ok_or_else(|| {
            StorageError::FormatMismatch("cannot save without schema".to_string())
        })?;
        let rows_have_undefined_cells = undefined_cells
            .iter()
            .any(|undefined| !undefined.is_empty());
        let filtered_rows_storage =
            rows_have_undefined_cells.then(|| filter_rows_for_save(rows, undefined_cells));
        let filtered_rows: &[RecordValue] = filtered_rows_storage.as_deref().unwrap_or(rows);
        let row_field_positions = (!rows_have_undefined_cells)
            .then(|| {
                filtered_rows.first().map(|row| {
                    row.fields()
                        .iter()
                        .enumerate()
                        .map(|(idx, field)| (field.name.clone(), idx))
                        .collect::<HashMap<_, _>>()
                })
            })
            .flatten();
        let nrrow = filtered_rows.len() as u64;
        let has_virtual = !virtual_bindings.is_empty();
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "prepare_rows",
                Some(format!(
                    "rows={} columns={} bindings={} virtual={}",
                    nrrow,
                    schema.columns().len(),
                    bindings.len(),
                    has_virtual
                )),
            );
        }

        // Group columns by DM kind. Columns in `bindings` get their own DM;
        // everything else goes into the default DM.
        struct DmGroup {
            dm_kind: DataManagerKind,
            dm_type_name: String,
            seq_nr: u32,
            col_names: Vec<String>,
            tile_shape: Option<Vec<usize>>,
        }

        let mut groups: Vec<DmGroup> = Vec::new();

        // Default DM group (seq_nr 0).
        let default_type_name = match default_dm {
            DataManagerKind::StManAipsIO => "StManAipsIO",
            DataManagerKind::StandardStMan => "StandardStMan",
            DataManagerKind::IncrementalStMan => "IncrementalStMan",
            DataManagerKind::TiledColumnStMan => "TiledColumnStMan",
            DataManagerKind::TiledShapeStMan => "TiledShapeStMan",
            DataManagerKind::TiledCellStMan => "TiledCellStMan",
            DataManagerKind::TiledDataStMan => "TiledDataStMan",
        };
        groups.push(DmGroup {
            dm_kind: default_dm,
            dm_type_name: default_type_name.to_string(),
            seq_nr: 0,
            col_names: Vec::new(),
            tile_shape: default_tile_shape.map(|s| s.to_vec()),
        });

        // Collect virtual column names for filtering.
        let virtual_col_names: HashSet<&str> = virtual_columns.iter().map(|s| s.as_str()).collect();

        // Build additional DM groups from bindings, then assign columns.
        let mut binding_seq_map: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let mut next_seq = 1u32;

        for col in schema.columns() {
            let col_name = col.name();
            if virtual_col_names.contains(col_name) {
                continue; // Virtual columns don't get stored DM entries.
            }
            if let Some(binding) = bindings.get(col_name) {
                let group_key = if matches!(
                    binding.data_manager,
                    DataManagerKind::TiledColumnStMan
                        | DataManagerKind::TiledShapeStMan
                        | DataManagerKind::TiledCellStMan
                        | DataManagerKind::TiledDataStMan
                ) {
                    format!("{:?}:{col_name}", binding.data_manager)
                } else {
                    format!("{:?}", binding.data_manager)
                };
                let group_idx = if let Some(&idx) = binding_seq_map.get(&group_key) {
                    idx
                } else {
                    let dm_type = match binding.data_manager {
                        DataManagerKind::StManAipsIO => "StManAipsIO",
                        DataManagerKind::StandardStMan => "StandardStMan",
                        DataManagerKind::IncrementalStMan => "IncrementalStMan",
                        DataManagerKind::TiledColumnStMan => "TiledColumnStMan",
                        DataManagerKind::TiledShapeStMan => "TiledShapeStMan",
                        DataManagerKind::TiledCellStMan => "TiledCellStMan",
                        DataManagerKind::TiledDataStMan => "TiledDataStMan",
                    };
                    let idx = groups.len();
                    groups.push(DmGroup {
                        dm_kind: binding.data_manager,
                        dm_type_name: dm_type.to_string(),
                        seq_nr: next_seq,
                        col_names: Vec::new(),
                        tile_shape: binding.tile_shape.clone(),
                    });
                    next_seq += 1;
                    binding_seq_map.insert(group_key, idx);
                    idx
                };
                groups[group_idx].col_names.push(col_name.to_string());
            } else {
                groups[0].col_names.push(col_name.to_string());
            }
        }
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "group_columns",
                Some(format!("groups={} next_seq={next_seq}", groups.len())),
            );
        }

        // Build a combined table.dat with all DM entries.
        // For now, use from_snapshot for the first DM and add extra DM entries manually.
        // This is a simplified approach: we write each group's data separately.

        // First, build a complete table_dat with all DMs.
        let mut dm_entries = Vec::new();
        let mut col_dm_map: std::collections::HashMap<String, u32> =
            std::collections::HashMap::new();

        for group in &groups {
            dm_entries.push(table_control::DataManagerEntry {
                type_name: group.dm_type_name.clone(),
                seq_nr: group.seq_nr,
                data: Vec::new(), // Will be filled in for SSM/ISM.
            });
            for col_name in &group.col_names {
                col_dm_map.insert(col_name.clone(), group.seq_nr);
            }
        }

        // Build initial table_dat (using from_snapshot but overriding DM entries).
        let mut table_dat = if has_virtual {
            TableDatContents::from_snapshot_with_virtual(
                schema,
                keywords,
                column_keywords,
                nrrow,
                &groups[0].dm_type_name,
                &[],
                big_endian,
                virtual_bindings,
                table_path,
            )
        } else {
            TableDatContents::from_snapshot(
                schema,
                keywords,
                column_keywords,
                nrrow,
                &groups[0].dm_type_name,
                &[],
                big_endian,
            )
        };
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark("build_table_dat");
        }

        // Override DM entries with our multi-DM setup.
        // Preserve any virtual DM entries that from_snapshot_with_virtual added.
        let virtual_dm_entries: Vec<_> = table_dat
            .column_set
            .data_managers
            .iter()
            .filter(|e| e.seq_nr > 0 && is_virtual_engine(&e.type_name))
            .cloned()
            .collect();

        // Update each PlainColumnEntry's dm_seq_nr.
        for pc in &mut table_dat.column_set.columns {
            if let Some(&seq) = col_dm_map.get(&pc.original_name) {
                pc.dm_seq_nr = seq;
            }
        }

        // Override DM entries, filtering out empty groups that have no
        // columns and no data file to avoid "missing table.f<N>" errors.
        let non_empty_groups: Vec<&DmGroup> =
            groups.iter().filter(|g| !g.col_names.is_empty()).collect();
        table_dat.column_set.data_managers = non_empty_groups
            .iter()
            .map(|g| table_control::DataManagerEntry {
                type_name: g.dm_type_name.clone(),
                seq_nr: g.seq_nr,
                data: Vec::new(),
            })
            .collect();
        table_dat
            .column_set
            .data_managers
            .extend(virtual_dm_entries);
        table_dat.column_set.seq_count = table_dat
            .column_set
            .data_managers
            .iter()
            .map(|e| e.seq_nr + 1)
            .max()
            .unwrap_or(1);
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "finalize_table_dat",
                Some(format!(
                    "non_empty_groups={} seq_count={}",
                    non_empty_groups.len(),
                    table_dat.column_set.seq_count
                )),
            );
        }

        // Write data files per group.
        for group in &groups {
            if group.col_names.is_empty() {
                continue;
            }
            let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, group.seq_nr));

            let group_col_set: HashSet<&str> = group.col_names.iter().map(|s| s.as_str()).collect();
            let group_col_descs: Vec<_> = table_dat
                .table_desc
                .columns
                .iter()
                .filter(|c| group_col_set.contains(c.col_name.as_str()))
                .cloned()
                .collect();
            let group_col_indices = row_field_positions.as_ref().and_then(|positions| {
                let indices: Vec<usize> = group_col_descs
                    .iter()
                    .filter_map(|desc| positions.get(&desc.col_name).copied())
                    .collect();
                (indices.len() == group_col_descs.len()).then_some(indices)
            });
            let use_borrowed_tiled_values = matches!(
                group.dm_kind,
                DataManagerKind::TiledColumnStMan
                    | DataManagerKind::TiledShapeStMan
                    | DataManagerKind::TiledCellStMan
                    | DataManagerKind::TiledDataStMan
            ) && group_col_descs.len() == 1;
            let override_columns: Vec<_> = group_col_descs
                .iter()
                .filter_map(|desc| {
                    column_overrides
                        .contains_key(&desc.col_name)
                        .then_some(desc.col_name.as_str())
                })
                .collect();
            let use_direct_indexed_rows = matches!(
                group.dm_kind,
                DataManagerKind::StandardStMan | DataManagerKind::IncrementalStMan
            ) && group_col_indices.is_some()
                && override_columns.is_empty();
            let group_col_index = group_col_indices
                .as_ref()
                .and_then(|indices| (indices.len() == 1).then_some(indices[0]));
            let group_values = if use_borrowed_tiled_values {
                if let Some(override_values) = column_overrides.get(&group_col_descs[0].col_name) {
                    Some(override_values.iter().map(|value| value.as_ref()).collect())
                } else {
                    Some(project_column_values_for_group(
                        filtered_rows,
                        &group_col_descs[0].col_name,
                        group_col_index,
                    ))
                }
            } else {
                None
            };
            let scalar_override_columns = if matches!(
                group.dm_kind,
                DataManagerKind::StandardStMan | DataManagerKind::IncrementalStMan
            ) {
                scalar_override_columns_for_group(&group_col_descs, column_overrides)?
            } else {
                None
            };
            let group_rows = if group_values.is_none()
                && !use_direct_indexed_rows
                && scalar_override_columns.is_none()
            {
                Some(project_rows_for_group(
                    filtered_rows,
                    &group_col_descs,
                    group_col_indices.as_deref(),
                    column_overrides,
                ))
            } else {
                None
            };
            if let Some(profiler) = profiler.as_mut() {
                profiler.mark_with_detail(
                    "group_projection",
                    Some(format!(
                        "seq={} dm={} cols={} rows={} indexed_projection={} mode={}",
                        group.seq_nr,
                        group.dm_type_name,
                        group_col_descs.len(),
                        filtered_rows.len(),
                        group_col_indices.is_some(),
                        if group_values.is_some() {
                            "borrowed_values"
                        } else if use_direct_indexed_rows {
                            "direct_rows"
                        } else if scalar_override_columns.is_some() {
                            "scalar_override_columns"
                        } else {
                            "projected_rows"
                        }
                    )),
                );
            }

            match group.dm_kind {
                DataManagerKind::StManAipsIO => {
                    write_stman_file(
                        &data_path,
                        &group_col_descs,
                        group_rows.as_ref().expect("row projection for StManAipsIO"),
                        ByteOrder::BigEndian,
                    )?;
                }
                DataManagerKind::StandardStMan => {
                    let dm_data =
                        if let Some(scalar_override_columns) = scalar_override_columns.as_ref() {
                            let scalar_refs: Vec<_> =
                                scalar_override_columns.iter().map(Vec::as_slice).collect();
                            write_ssm_file_scalar_columns(
                                &data_path,
                                &group_col_descs,
                                &scalar_refs,
                                big_endian,
                            )?
                        } else if let Some(group_col_indices) = group_col_indices.as_ref() {
                            write_ssm_file_indexed(
                                &data_path,
                                &group_col_descs,
                                filtered_rows,
                                group_col_indices,
                                big_endian,
                            )?
                        } else {
                            write_ssm_file(
                                &data_path,
                                &group_col_descs,
                                group_rows
                                    .as_ref()
                                    .expect("row projection for StandardStMan"),
                                big_endian,
                            )?
                        };
                    // Update the DM blob in the table_dat.
                    if let Some(entry) = table_dat
                        .column_set
                        .data_managers
                        .iter_mut()
                        .find(|e| e.seq_nr == group.seq_nr)
                    {
                        entry.data = dm_data;
                    }
                }
                DataManagerKind::IncrementalStMan => {
                    let dm_data =
                        if let Some(scalar_override_columns) = scalar_override_columns.as_ref() {
                            let scalar_refs: Vec<_> =
                                scalar_override_columns.iter().map(Vec::as_slice).collect();
                            write_ism_file_scalar_columns(
                                &data_path,
                                &group_col_descs,
                                &scalar_refs,
                                big_endian,
                            )?
                        } else if let Some(group_col_indices) = group_col_indices.as_ref() {
                            write_ism_file_indexed(
                                &data_path,
                                &group_col_descs,
                                filtered_rows,
                                group_col_indices,
                                big_endian,
                            )?
                        } else {
                            write_ism_file(
                                &data_path,
                                &group_col_descs,
                                group_rows
                                    .as_ref()
                                    .expect("row projection for IncrementalStMan"),
                                big_endian,
                            )?
                        };
                    if let Some(entry) = table_dat
                        .column_set
                        .data_managers
                        .iter_mut()
                        .find(|e| e.seq_nr == group.seq_nr)
                    {
                        entry.data = dm_data;
                    }
                }
                DataManagerKind::TiledColumnStMan
                | DataManagerKind::TiledShapeStMan
                | DataManagerKind::TiledCellStMan
                | DataManagerKind::TiledDataStMan => {
                    // Use the first column name as the hypercolumn/DM name.
                    let first_col = group.col_names.first().map(|s| s.as_str()).unwrap_or("");
                    if let Some(group_values) = group_values.as_ref() {
                        tiled_stman::save_tiled_single_column_values(
                            table_path,
                            group.seq_nr,
                            &group_col_descs[0],
                            group_values,
                            tiled_stman::SingleColumnTiledSaveOptions {
                                dm_type_name: &group.dm_type_name,
                                big_endian,
                                default_tile_shape: group.tile_shape.as_deref(),
                                dm_name: first_col,
                            },
                        )?;
                    } else {
                        tiled_stman::save_tiled_columns(
                            table_path,
                            group.seq_nr,
                            &group.dm_type_name,
                            &group_col_descs,
                            group_rows
                                .as_ref()
                                .expect("row projection for tiled storage"),
                            big_endian,
                            group.tile_shape.as_deref(),
                            first_col,
                        )?;
                    }
                }
            }
            if let Some(profiler) = profiler.as_mut() {
                profiler.mark_with_detail(
                    "group_save",
                    Some(format!(
                        "seq={} dm={} cols={} rows={}",
                        group.seq_nr,
                        group.dm_type_name,
                        group_col_descs.len(),
                        filtered_rows.len()
                    )),
                );
            }
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        write_table_dat(&control_path, &table_dat)?;
        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, table_info.to_string())?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark("write_control_files");
        }

        Ok(())
    }

    /// Save with per-column DM bindings using an owned snapshot.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn save_with_bindings(
        &self,
        table_path: &Path,
        snapshot: &StorageSnapshot,
        default_dm: crate::table::DataManagerKind,
        big_endian: bool,
        default_tile_shape: Option<&[usize]>,
        bindings: &std::collections::HashMap<String, crate::table::ColumnBinding>,
    ) -> Result<(), StorageError> {
        self.save_with_bindings_borrowed(
            table_path,
            &snapshot.rows,
            &snapshot.undefined_cells,
            &snapshot.keywords,
            &snapshot.column_keywords,
            snapshot.schema.as_ref(),
            &snapshot.table_info,
            &snapshot.virtual_columns,
            &snapshot.virtual_bindings,
            default_dm,
            big_endian,
            default_tile_shape,
            bindings,
        )
    }
}

fn scalar_columns_from_snapshot(snapshot: &StorageSnapshot) -> ScalarColumnSnapshot {
    let mut columns: HashMap<String, Vec<Option<ScalarValue>>> = HashMap::new();
    for (row_index, row) in snapshot.rows.iter().enumerate() {
        for (name, values) in &mut columns {
            values.push(match row.get(name) {
                Some(Value::Scalar(value)) => Some(value.clone()),
                _ => None,
            });
        }
        for field in row.fields() {
            if let Value::Scalar(value) = &field.value {
                let entry = columns
                    .entry(field.name.clone())
                    .or_insert_with(|| vec![None; row_index]);
                entry.push(Some(value.clone()));
            }
        }
    }
    for values in columns.values_mut() {
        if values.len() < snapshot.row_count {
            values.resize(snapshot.row_count, None);
        }
    }
    ScalarColumnSnapshot {
        row_count: snapshot.row_count,
        columns,
    }
}

fn array_column_from_snapshot(
    snapshot: &StorageSnapshot,
    column: &str,
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    snapshot
        .rows
        .iter()
        .map(|row| match row.get(column) {
            Some(Value::Array(value)) => Ok(Some(value.clone())),
            Some(other) => Err(StorageError::FormatMismatch(format!(
                "column '{column}' expected array value, found {:?}",
                other.kind()
            ))),
            None => Ok(None),
        })
        .collect()
}

fn scalar_column_from_snapshot(
    snapshot: &StorageSnapshot,
    column: &str,
) -> Result<Vec<Option<ScalarValue>>, StorageError> {
    snapshot
        .rows
        .iter()
        .map(|row| match row.get(column) {
            Some(Value::Scalar(value)) => Ok(Some(value.clone())),
            Some(other) => Err(StorageError::FormatMismatch(format!(
                "column '{column}' expected scalar value, found {:?}",
                other.kind()
            ))),
            None => Ok(None),
        })
        .collect()
}

fn select_array_rows(
    values: &[Option<ArrayValue>],
    selected_rows: &[usize],
) -> Vec<Option<ArrayValue>> {
    selected_rows
        .iter()
        .map(|&row_idx| values.get(row_idx).cloned().unwrap_or(None))
        .collect()
}

pub(crate) fn slice_array_value_2d_channel_range(
    value: ArrayValue,
    channel_start: usize,
    channel_count: usize,
) -> Result<ArrayValue, StorageError> {
    macro_rules! slice_variant {
        ($values:expr, $ctor:expr) => {{
            let shape = $values.shape().to_vec();
            if shape.len() != 2 {
                return Err(StorageError::FormatMismatch(format!(
                    "2-D channel-range array read expected rank-2 array, found shape {shape:?}"
                )));
            }
            let Some(channel_end) = channel_start.checked_add(channel_count) else {
                return Err(StorageError::FormatMismatch(
                    "2-D channel-range array read overflowed channel bounds".to_string(),
                ));
            };
            if channel_end > shape[1] {
                return Err(StorageError::FormatMismatch(format!(
                    "2-D channel range {channel_start}..{channel_end} exceeds array channel axis with {} channels",
                    shape[1]
                )));
            }
            $ctor(
                $values
                    .slice_axis(Axis(1), Slice::from(channel_start..channel_end))
                    .to_owned(),
            )
        }};
    }

    Ok(match value {
        ArrayValue::Bool(values) => slice_variant!(values, ArrayValue::Bool),
        ArrayValue::UInt8(values) => slice_variant!(values, ArrayValue::UInt8),
        ArrayValue::Int16(values) => slice_variant!(values, ArrayValue::Int16),
        ArrayValue::UInt16(values) => slice_variant!(values, ArrayValue::UInt16),
        ArrayValue::Int32(values) => slice_variant!(values, ArrayValue::Int32),
        ArrayValue::UInt32(values) => slice_variant!(values, ArrayValue::UInt32),
        ArrayValue::Int64(values) => slice_variant!(values, ArrayValue::Int64),
        ArrayValue::Float32(values) => slice_variant!(values, ArrayValue::Float32),
        ArrayValue::Float64(values) => slice_variant!(values, ArrayValue::Float64),
        ArrayValue::Complex32(values) => slice_variant!(values, ArrayValue::Complex32),
        ArrayValue::Complex64(values) => slice_variant!(values, ArrayValue::Complex64),
        ArrayValue::String(values) => slice_variant!(values, ArrayValue::String),
    })
}

fn select_scalar_rows(
    values: &[Option<ScalarValue>],
    selected_rows: &[usize],
) -> Vec<Option<ScalarValue>> {
    selected_rows
        .iter()
        .map(|&row_idx| values.get(row_idx).cloned().unwrap_or(None))
        .collect()
}

fn collect_stman_scalar_columns(
    data_path: &Path,
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    requested_columns: Option<&HashSet<&str>>,
    nrrow: usize,
    byte_order: ByteOrder,
    columns: &mut HashMap<String, Vec<Option<ScalarValue>>>,
) -> Result<(), StorageError> {
    let col_info: Vec<StManColumnInfo> = bound_cols
        .iter()
        .map(|(desc_idx, _)| {
            let c = &all_col_descs[*desc_idx];
            let nrelem = if c.is_array && !c.shape.is_empty() {
                c.shape.iter().map(|&s| s as usize).product()
            } else {
                0
            };
            StManColumnInfo {
                is_array: c.is_array,
                nrelem,
            }
        })
        .collect();
    let stman_data = read_stman_file(data_path, &col_info, byte_order)?;
    for (stman_col_idx, (desc_idx, _)) in bound_cols.iter().enumerate() {
        if stman_col_idx >= stman_data.columns.len() {
            break;
        }
        let col_desc = &all_col_descs[*desc_idx];
        if requested_columns
            .is_some_and(|requested| !requested.contains(col_desc.col_name.as_str()))
        {
            continue;
        }
        if col_desc.is_array || col_desc.is_record() {
            continue;
        }
        let values =
            scalar_values_from_stman_data(&stman_data.columns[stman_col_idx], col_desc, nrrow)?;
        columns.insert(col_desc.col_name.clone(), values);
    }
    Ok(())
}

fn collect_ssm_scalar_columns(
    data_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    nrrow: usize,
    columns: &mut HashMap<String, Vec<Option<ScalarValue>>>,
) -> Result<(), StorageError> {
    let col_descs: Vec<&table_control::ColumnDescContents> = bound_cols
        .iter()
        .map(|(desc_idx, _)| &all_col_descs[*desc_idx])
        .collect();
    let ssm_columns = read_ssm_file(data_path, dm_blob, &col_descs, nrrow)?;
    for (col_name, col_result) in &ssm_columns {
        let col_desc = col_descs
            .iter()
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "SSM returned column '{col_name}' not in descriptor"
                ))
            })?;
        if col_desc.is_array || col_desc.is_record() {
            continue;
        }
        let values = scalar_values_from_ssm_data(col_result, col_desc, nrrow)?;
        columns.insert(col_name.clone(), values);
    }
    Ok(())
}

fn collect_ism_scalar_columns(
    data_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    nrrow: usize,
    columns: &mut HashMap<String, Vec<Option<ScalarValue>>>,
) -> Result<(), StorageError> {
    let col_descs: Vec<&table_control::ColumnDescContents> = bound_cols
        .iter()
        .map(|(desc_idx, _)| &all_col_descs[*desc_idx])
        .collect();
    let ism_columns = read_ism_file(data_path, dm_blob, &col_descs, nrrow)?;
    for (col_name, col_result) in &ism_columns {
        let col_desc = col_descs
            .iter()
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "ISM returned column '{col_name}' not in descriptor"
                ))
            })?;
        if col_desc.is_array || col_desc.is_record() {
            continue;
        }
        let values = scalar_values_from_ism_data(col_result, col_desc, nrrow)?;
        columns.insert(col_name.clone(), values);
    }
    Ok(())
}

fn scalar_values_from_stman_data(
    data: &StManColumnData,
    col_desc: &table_control::ColumnDescContents,
    nrrow: usize,
) -> Result<Vec<Option<ScalarValue>>, StorageError> {
    match data {
        StManColumnData::Flat(raw) => {
            let mut values = Vec::with_capacity(nrrow);
            for row_idx in 0..nrrow {
                let value = extract_row_value(raw, col_desc, row_idx, nrrow)?;
                let scalar = scalar_value_from_value(value, &col_desc.col_name)?;
                if (col_desc.option & 2) != 0
                    && scalar_value_is_default(
                        &Value::Scalar(scalar.clone()),
                        col_desc.require_primitive_type()?,
                    )
                {
                    values.push(None);
                } else {
                    values.push(Some(scalar));
                }
            }
            Ok(values)
        }
        StManColumnData::Indirect(per_row) => {
            scalar_values_from_indirect(per_row, &col_desc.col_name)
        }
    }
}

fn scalar_values_from_ssm_data(
    data: &standard_stman::SsmColumnResult,
    col_desc: &table_control::ColumnDescContents,
    nrrow: usize,
) -> Result<Vec<Option<ScalarValue>>, StorageError> {
    match data {
        standard_stman::SsmColumnResult::Flat(raw) => {
            let mut values = Vec::with_capacity(nrrow);
            for row_idx in 0..nrrow {
                let value = extract_row_value(raw, col_desc, row_idx, nrrow)?;
                values.push(Some(scalar_value_from_value(value, &col_desc.col_name)?));
            }
            Ok(values)
        }
        standard_stman::SsmColumnResult::Indirect(per_row) => {
            scalar_values_from_indirect(per_row, &col_desc.col_name)
        }
    }
}

fn scalar_values_from_ism_data(
    data: &IsmColumnResult,
    col_desc: &table_control::ColumnDescContents,
    nrrow: usize,
) -> Result<Vec<Option<ScalarValue>>, StorageError> {
    match data {
        IsmColumnResult::Flat(raw) => {
            let mut values = Vec::with_capacity(nrrow);
            for row_idx in 0..nrrow {
                let value = extract_row_value(raw, col_desc, row_idx, nrrow)?;
                values.push(Some(scalar_value_from_value(value, &col_desc.col_name)?));
            }
            Ok(values)
        }
        IsmColumnResult::Indirect(per_row) => {
            scalar_values_from_indirect(per_row, &col_desc.col_name)
        }
    }
}

fn scalar_values_from_indirect(
    per_row: &[Option<Value>],
    column: &str,
) -> Result<Vec<Option<ScalarValue>>, StorageError> {
    per_row
        .iter()
        .map(|value| {
            value
                .clone()
                .map(|value| scalar_value_from_value(value, column))
                .transpose()
        })
        .collect()
}

fn scalar_value_from_value(value: Value, column: &str) -> Result<ScalarValue, StorageError> {
    match value {
        Value::Scalar(scalar) => Ok(scalar),
        other => Err(StorageError::FormatMismatch(format!(
            "column '{column}' expected scalar value, found {:?}",
            other.kind()
        ))),
    }
}

/// Load columns from a StManAipsIO data file into row records.
fn load_stman_aipsio_columns(
    data_path: &Path,
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    rows: &mut [RecordValue],
    undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
    byte_order: ByteOrder,
) -> Result<(), StorageError> {
    // Build column info for the StManAipsIO reader using the bound columns
    let col_info: Vec<StManColumnInfo> = bound_cols
        .iter()
        .map(|(desc_idx, _)| {
            let c = &all_col_descs[*desc_idx];
            let nrelem = if c.is_array && !c.shape.is_empty() {
                c.shape.iter().map(|&s| s as usize).product()
            } else {
                0
            };
            StManColumnInfo {
                is_array: c.is_array,
                nrelem,
            }
        })
        .collect();

    let stman_data = read_stman_file(data_path, &col_info, byte_order)?;

    for (stman_col_idx, (desc_idx, _)) in bound_cols.iter().enumerate() {
        if stman_col_idx >= stman_data.columns.len() {
            break;
        }
        let col_desc = &all_col_descs[*desc_idx];
        match &stman_data.columns[stman_col_idx] {
            StManColumnData::Flat(raw) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = extract_row_value(raw, col_desc, row_idx, nrrow)?;
                    if !col_desc.is_array
                        && (col_desc.option & 2) != 0
                        && scalar_value_is_default(&value, col_desc.require_primitive_type()?)
                    {
                        undefined_cells[row_idx].insert(col_desc.col_name.clone());
                        continue;
                    }
                    if matches!(&value, Value::Array(_)) {
                        undefined_cells[row_idx].insert(col_desc.col_name.clone());
                    }
                    row.push(RecordField::new(col_desc.col_name.clone(), value));
                }
            }
            StManColumnData::Indirect(per_row) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = match per_row.get(row_idx) {
                        Some(Some(v)) => {
                            if col_desc.is_record() {
                                // C++ stores records as indirect Vector<uChar>.
                                // Deserialize the byte array back to a RecordValue.
                                match v {
                                    Value::Array(casa_types::ArrayValue::UInt8(arr)) => {
                                        let bytes = arr.as_slice().ok_or_else(|| {
                                            StorageError::FormatMismatch(format!(
                                                "record column '{}': non-contiguous u8 array",
                                                col_desc.col_name
                                            ))
                                        })?;
                                        Value::Record(table_control::deserialize_record_from_uchar(
                                            bytes,
                                        )?)
                                    }
                                    Value::Record(_) => v.clone(),
                                    _ => {
                                        return Err(StorageError::FormatMismatch(format!(
                                            "record column '{}' has unexpected value type",
                                            col_desc.col_name
                                        )));
                                    }
                                }
                            } else {
                                v.clone()
                            }
                        }
                        // Undefined cell: empty record or 0-D empty array.
                        Some(None) | None => {
                            if col_desc.is_record() {
                                Value::Record(RecordValue::default())
                            } else {
                                let dt = CasacoreDataType::from_primitive_type(
                                    col_desc.require_primitive_type()?,
                                    false,
                                );
                                undefined_cells[row_idx].insert(col_desc.col_name.clone());
                                make_undefined_array(dt, col_desc.nrdim.max(0) as usize)
                            }
                        }
                    };
                    row.push(RecordField::new(col_desc.col_name.clone(), value));
                }
            }
        }
    }

    Ok(())
}

/// Load columns from a StandardStMan data file into row records.
fn load_ssm_columns(
    data_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    rows: &mut [RecordValue],
    undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
) -> Result<(), StorageError> {
    let col_descs: Vec<&table_control::ColumnDescContents> = bound_cols
        .iter()
        .map(|(desc_idx, _)| &all_col_descs[*desc_idx])
        .collect();

    let ssm_columns = read_ssm_file(data_path, dm_blob, &col_descs, nrrow)?;

    for (col_name, col_result) in &ssm_columns {
        let col_desc = col_descs
            .iter()
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "SSM returned column '{col_name}' not in descriptor"
                ))
            })?;

        match col_result {
            standard_stman::SsmColumnResult::Flat(raw_data) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = extract_row_value(raw_data, col_desc, row_idx, nrrow)?;
                    row.push(RecordField::new(col_name.clone(), value));
                }
            }
            standard_stman::SsmColumnResult::Indirect(per_row) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = match per_row.get(row_idx) {
                        Some(Some(v)) => v.clone(),
                        Some(None) | None => {
                            if col_desc.is_record() {
                                Value::Record(RecordValue::default())
                            } else {
                                let dt = CasacoreDataType::from_primitive_type(
                                    col_desc.require_primitive_type()?,
                                    false,
                                );
                                undefined_cells[row_idx].insert(col_desc.col_name.clone());
                                make_undefined_array(dt, col_desc.nrdim.max(0) as usize)
                            }
                        }
                    };
                    row.push(RecordField::new(col_name.clone(), value));
                }
            }
        }
    }

    Ok(())
}

/// Load columns from an IncrementalStMan data file into row records.
fn load_ism_columns(
    data_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    rows: &mut [RecordValue],
    undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
) -> Result<(), StorageError> {
    let col_descs: Vec<&table_control::ColumnDescContents> = bound_cols
        .iter()
        .map(|(desc_idx, _)| &all_col_descs[*desc_idx])
        .collect();

    let ism_columns = read_ism_file(data_path, dm_blob, &col_descs, nrrow)?;

    for (col_name, col_result) in &ism_columns {
        let col_desc = col_descs
            .iter()
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "ISM returned column '{col_name}' not in descriptor"
                ))
            })?;

        match col_result {
            IsmColumnResult::Flat(raw_data) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = extract_row_value(raw_data, col_desc, row_idx, nrrow)?;
                    row.push(RecordField::new(col_name.clone(), value));
                }
            }
            IsmColumnResult::Indirect(per_row) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    let value = match per_row.get(row_idx) {
                        Some(Some(value)) => value.clone(),
                        Some(None) | None => {
                            let dt = CasacoreDataType::from_primitive_type(
                                col_desc.require_primitive_type()?,
                                false,
                            );
                            undefined_cells[row_idx].insert(col_desc.col_name.clone());
                            make_undefined_array(dt, col_desc.nrdim.max(0) as usize)
                        }
                    };
                    row.push(RecordField::new(col_name.clone(), value));
                }
            }
        }
    }

    Ok(())
}

/// Create a Value representing an undefined variable-shape array cell.
///
/// For ndim > 0, returns a zero-sized array with `ndim` dimensions (all sizes 0).
/// For ndim == 0, returns a 0-D empty array.
pub(crate) fn make_undefined_array(dt: CasacoreDataType, ndim: usize) -> casa_types::Value {
    use casa_types::{ArrayValue, Value};
    use ndarray::{ArrayD, IxDyn};

    let shape = vec![0usize; ndim];
    let s = IxDyn(&shape);
    let av = match dt {
        CasacoreDataType::TpBool => ArrayValue::Bool(ArrayD::default(s)),
        CasacoreDataType::TpUChar => ArrayValue::UInt8(ArrayD::default(s)),
        CasacoreDataType::TpShort => ArrayValue::Int16(ArrayD::default(s)),
        CasacoreDataType::TpUShort => ArrayValue::UInt16(ArrayD::default(s)),
        CasacoreDataType::TpInt => ArrayValue::Int32(ArrayD::default(s)),
        CasacoreDataType::TpUInt => ArrayValue::UInt32(ArrayD::default(s)),
        CasacoreDataType::TpInt64 => ArrayValue::Int64(ArrayD::default(s)),
        CasacoreDataType::TpFloat => ArrayValue::Float32(ArrayD::default(s)),
        CasacoreDataType::TpDouble => ArrayValue::Float64(ArrayD::default(s)),
        CasacoreDataType::TpComplex => {
            ArrayValue::Complex32(ArrayD::from_elem(s, casa_types::Complex32::new(0.0, 0.0)))
        }
        CasacoreDataType::TpDComplex => {
            ArrayValue::Complex64(ArrayD::from_elem(s, casa_types::Complex64::new(0.0, 0.0)))
        }
        CasacoreDataType::TpString => ArrayValue::String(ArrayD::from_elem(s, String::new())),
        _ => ArrayValue::Float32(ArrayD::default(s)),
    };
    Value::Array(av)
}

/// Compute a relative path using the C++ `Path::stripDirectory` convention.
///
/// Given `target_path` (the table being referenced) and `from_path` (the table
/// that contains the reference), produces a relative path string that
/// [`add_directory`] can invert.
///
/// Convention:
/// - If target is a sibling (same parent directory): `"./name"`
/// - If target is inside the from directory: `"././name"`
/// - Otherwise: absolute path as fallback
pub(crate) fn strip_directory(target_path: &Path, from_path: &Path) -> String {
    let target_abs = std::path::absolute(target_path).unwrap_or_else(|_| target_path.to_path_buf());
    let from_abs = std::path::absolute(from_path).unwrap_or_else(|_| from_path.to_path_buf());

    // Check if target is inside the from directory (././ convention).
    let from_dir_prefix = format!("{}/", from_abs.display());
    let target_str = format!("{}", target_abs.display());
    if target_str.starts_with(&from_dir_prefix) {
        let remainder = &target_str[from_dir_prefix.len()..];
        return format!("././{remainder}");
    }

    // Check if they share the same parent directory (./ convention).
    if let (Some(from_parent), Some(target_parent)) = (from_abs.parent(), target_abs.parent()) {
        if from_parent == target_parent {
            let name = target_abs.file_name().unwrap_or_default().to_string_lossy();
            return format!("./{name}");
        }
    }

    // Fallback: absolute path.
    target_abs.to_string_lossy().to_string()
}

/// Resolve a stored relative path using the C++ `Path::addDirectory` convention.
///
/// - `"./name"` → `parent_dir(ref_table_path) / name`
/// - `"././name"` → `ref_table_path / name`
/// - absolute or plain → used as-is
pub(crate) fn add_directory(
    relative: &str,
    ref_table_path: &Path,
) -> Result<PathBuf, StorageError> {
    let mut name = relative;
    let mut stripped_count = 0usize;

    // Strip leading "./" segments, counting how many were removed.
    while name.len() >= 2 && name.starts_with("./") {
        name = &name[2..];
        stripped_count += 1;
    }

    if stripped_count == 0 {
        // No "./" prefix — use as-is (absolute or plain relative).
        let p = PathBuf::from(relative);
        if p.is_absolute() {
            return Ok(p);
        }
        // Plain relative: resolve relative to ref table's parent directory.
        return Ok(ref_table_path
            .parent()
            .unwrap_or(ref_table_path)
            .join(relative));
    }

    if stripped_count == 1 {
        // "./" was removed → add parent directory of ref_table_path.
        let dir = ref_table_path.parent().ok_or_else(|| {
            StorageError::FormatMismatch(format!(
                "cannot get parent directory of '{}'",
                ref_table_path.display()
            ))
        })?;
        return Ok(dir.join(name));
    }

    // "././" or more → add the ref table path itself (subtable inside it).
    Ok(ref_table_path.join(name))
}

/// Read and parse the `table.info` file from a table directory.
///
/// Returns a default `TableInfo` if the file is missing or unreadable.
fn load_table_info(table_path: &Path) -> TableInfo {
    let info_path = table_path.join(TABLE_INFO_FILE);
    match fs::read_to_string(&info_path) {
        Ok(contents) => TableInfo::parse(&contents),
        Err(_) => TableInfo::default(),
    }
}

/// Build [`DataManagerInfo`] from the parsed table.dat contents.
fn extract_dm_info(table_dat: &table_control::TableDatContents) -> Vec<DataManagerInfo> {
    table_dat
        .column_set
        .data_managers
        .iter()
        .map(|dm| {
            let columns: Vec<String> = table_dat
                .column_set
                .columns
                .iter()
                .filter(|pc| pc.dm_seq_nr == dm.seq_nr)
                .map(|pc| pc.original_name.clone())
                .collect();
            DataManagerInfo {
                dm_type: dm.type_name.clone(),
                seq_nr: dm.seq_nr,
                columns,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::TableInfo;

    #[test]
    fn table_info_parse_round_trip() {
        let info = TableInfo {
            table_type: "MeasurementSet".to_string(),
            sub_type: "UVFITS".to_string(),
        };
        let text = info.to_string();
        let parsed = TableInfo::parse(&text);
        assert_eq!(info, parsed);
    }

    #[test]
    fn table_info_parse_empty() {
        let parsed = TableInfo::parse("");
        assert_eq!(parsed, TableInfo::default());
    }

    #[test]
    fn table_info_parse_type_only() {
        let parsed = TableInfo::parse("Type = Catalog\n");
        assert_eq!(parsed.table_type, "Catalog");
        assert_eq!(parsed.sub_type, "");
    }
}
