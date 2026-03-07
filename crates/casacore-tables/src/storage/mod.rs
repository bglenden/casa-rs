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
pub use tiled_stman::{TilePixel, TiledFileIO};
pub(crate) mod virtual_bitflags;
pub(crate) mod virtual_compress;
pub(crate) mod virtual_engine;
pub(crate) mod virtual_forward;
pub(crate) mod virtual_scaled_array;
pub(crate) mod virtual_taql_column;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use casacore_aipsio::ByteOrder;
use casacore_types::{RecordField, RecordValue, Value};
use thiserror::Error;

use crate::schema::{SchemaError, TableSchema};

use self::data_type::CasacoreDataType;
use self::incremental_stman::{read_ism_file, write_ism_file};
use self::standard_stman::{read_ssm_file, write_ssm_file};
use self::stman_aipsio::{
    StManColumnData, StManColumnInfo, extract_row_value, read_stman_file, write_stman_file,
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

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("table path does not exist: {0}")]
    MissingPath(PathBuf),
    #[error("table control file is missing: {0}")]
    MissingControlFile(PathBuf),
    #[error("table data file is missing: {0}")]
    MissingDataFile(PathBuf),
    #[error("format mismatch: {0}")]
    FormatMismatch(String),
    #[error("schema error: {0}")]
    Schema(String),
    #[error("unsupported data manager: {0}")]
    UnsupportedDataManager(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("aipsio error: {0}")]
    AipsIo(#[from] casacore_aipsio::AipsIoObjectError),
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
    pub(crate) rows: Vec<RecordValue>,
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

/// Composite storage manager that dispatches per data manager type.
///
/// Reads `table.dat` once, then routes each DM's columns to the appropriate
/// reader (StManAipsIO or StandardStMan). Writes use StManAipsIO by default.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct CompositeStorage;

impl StorageManager for CompositeStorage {
    fn load(&self, table_path: &Path) -> Result<StorageSnapshot, StorageError> {
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !control_path.is_file() {
            return Err(StorageError::MissingControlFile(control_path));
        }

        match read_table_dat_dispatch(&control_path)? {
            TableDatResult::Plain(table_dat) => self.load_plain_table(table_path, &table_dat),
            TableDatResult::Ref(ref_dat) => self.load_ref_table(table_path, &ref_dat),
            TableDatResult::Concat(concat_dat) => self.load_concat_table(table_path, &concat_dat),
        }
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

        let nrrow = snapshot.rows.len() as u64;
        let data_path = table_path.join(format!("{}0", TABLE_DATA_FILE_PREFIX));
        let has_virtual = !snapshot.virtual_bindings.is_empty();

        // When saving with virtual bindings, build rows that exclude virtual columns.
        let stored_rows: Vec<RecordValue>;
        let rows_for_data = if has_virtual {
            stored_rows = snapshot
                .rows
                .iter()
                .map(|row| {
                    let stored_fields: Vec<_> = row
                        .fields()
                        .iter()
                        .filter(|f| !snapshot.virtual_columns.contains(&f.name))
                        .cloned()
                        .collect();
                    RecordValue::new(stored_fields)
                })
                .collect();
            &stored_rows
        } else {
            &snapshot.rows
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
    /// Load a PlainTable from table.dat contents and data files.
    ///
    /// Uses two-pass loading:
    /// - Pass 1: Load stored columns from storage managers (StManAipsIO, StandardStMan).
    /// - Pass 2: Materialize virtual columns from virtual engines.
    /// - Pass 3: Reject any remaining unknown DM types.
    fn load_plain_table(
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
        let nrrow = table_dat.nrrow as usize;

        let mut rows: Vec<RecordValue> = (0..nrrow).map(|_| RecordValue::default()).collect();
        let mut virtual_columns = HashSet::new();

        // Pass 1: Load stored columns from storage managers.
        for dm in &table_dat.column_set.data_managers {
            if is_virtual_engine(&dm.type_name) {
                continue; // Handled in pass 2.
            }

            let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, dm.seq_nr));

            let bound_cols: Vec<(usize, &_)> = table_dat
                .column_set
                .columns
                .iter()
                .enumerate()
                .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
                .collect();

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
                        nrrow,
                    )?;
                }
                other => {
                    return Err(StorageError::UnsupportedDataManager(other.to_string()));
                }
            }
        }

        // Pass 2: Materialize virtual columns.
        // Take a snapshot of stored rows so virtual engines can read from them
        // while we mutate the main rows vector.
        let stored_rows = rows.clone();
        for dm in &table_dat.column_set.data_managers {
            if !is_virtual_engine(&dm.type_name) {
                continue; // Already handled in pass 1.
            }

            let engine = lookup_engine(&dm.type_name)
                .ok_or_else(|| StorageError::UnsupportedDataManager(dm.type_name.clone()))?;

            let bound_cols: Vec<(usize, &_)> = table_dat
                .column_set
                .columns
                .iter()
                .enumerate()
                .filter(|(_, pc)| pc.dm_seq_nr == dm.seq_nr)
                .collect();

            // Record which columns are virtual.
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

        let table_info = load_table_info(table_path);

        // Build DM info from table.dat entries.
        let dm_info = extract_dm_info(table_dat);

        Ok(StorageSnapshot {
            rows,
            keywords,
            column_keywords,
            schema: Some(schema),
            table_info,
            virtual_columns,
            virtual_bindings: Vec::new(),
            dm_info,
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
                }
                rows.push(projected);
            } else {
                rows.push(parent.rows[idx].clone());
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
            rows,
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

        for (i, rel_path) in concat_dat.table_paths.iter().enumerate() {
            let abs_path = add_directory(rel_path, table_path)?;
            let sub_snapshot = self.load(&abs_path)?;

            if i == 0 {
                schema = sub_snapshot.schema;
                keywords = sub_snapshot.keywords;
                column_keywords = sub_snapshot.column_keywords;
            }
            all_rows.extend(sub_snapshot.rows);
        }

        let table_info = load_table_info(table_path);

        Ok(StorageSnapshot {
            rows: all_rows,
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
    pub(crate) fn save_with_bindings(
        &self,
        table_path: &Path,
        snapshot: &StorageSnapshot,
        default_dm: crate::table::DataManagerKind,
        big_endian: bool,
        default_tile_shape: Option<&[usize]>,
        bindings: &std::collections::HashMap<String, crate::table::ColumnBinding>,
    ) -> Result<(), StorageError> {
        use crate::table::DataManagerKind;

        fs::create_dir_all(table_path)?;

        let schema = snapshot.schema.as_ref().ok_or_else(|| {
            StorageError::FormatMismatch("cannot save without schema".to_string())
        })?;

        let nrrow = snapshot.rows.len() as u64;
        let has_virtual = !snapshot.virtual_bindings.is_empty();

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
        let virtual_col_names: HashSet<&str> = snapshot
            .virtual_columns
            .iter()
            .map(|s| s.as_str())
            .collect();

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
                let group_key = format!("{:?}", binding.data_manager);
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
                &snapshot.keywords,
                &snapshot.column_keywords,
                nrrow,
                &groups[0].dm_type_name,
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
                &groups[0].dm_type_name,
                &[],
                big_endian,
            )
        };

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

        // Write data files per group.
        for group in &groups {
            if group.col_names.is_empty() {
                continue;
            }
            let data_path = table_path.join(format!("{}{}", TABLE_DATA_FILE_PREFIX, group.seq_nr));

            // Filter rows and col_descs to only this group's columns.
            let group_col_set: HashSet<&str> = group.col_names.iter().map(|s| s.as_str()).collect();
            let group_rows: Vec<RecordValue> = snapshot
                .rows
                .iter()
                .map(|row| {
                    let fields: Vec<_> = row
                        .fields()
                        .iter()
                        .filter(|f| group_col_set.contains(f.name.as_str()))
                        .cloned()
                        .collect();
                    RecordValue::new(fields)
                })
                .collect();
            let group_col_descs: Vec<_> = table_dat
                .table_desc
                .columns
                .iter()
                .filter(|c| group_col_set.contains(c.col_name.as_str()))
                .cloned()
                .collect();

            match group.dm_kind {
                DataManagerKind::StManAipsIO => {
                    write_stman_file(
                        &data_path,
                        &group_col_descs,
                        &group_rows,
                        ByteOrder::BigEndian,
                    )?;
                }
                DataManagerKind::StandardStMan => {
                    let dm_data =
                        write_ssm_file(&data_path, &group_col_descs, &group_rows, big_endian)?;
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
                        write_ism_file(&data_path, &group_col_descs, &group_rows, big_endian)?;
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
                    tiled_stman::save_tiled_columns(
                        table_path,
                        group.seq_nr,
                        &group.dm_type_name,
                        &group_col_descs,
                        &group_rows,
                        big_endian,
                        group.tile_shape.as_deref(),
                        first_col,
                    )?;
                }
            }
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        write_table_dat(&control_path, &table_dat)?;
        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, snapshot.table_info.to_string())?;

        Ok(())
    }
}

/// Load columns from a StManAipsIO data file into row records.
fn load_stman_aipsio_columns(
    data_path: &Path,
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    rows: &mut [RecordValue],
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
                                    Value::Array(casacore_types::ArrayValue::UInt8(arr)) => {
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
                                make_undefined_array(dt, col_desc.nrdim as usize)
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
                                make_undefined_array(dt, col_desc.nrdim as usize)
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
    nrrow: usize,
) -> Result<(), StorageError> {
    let col_descs: Vec<&table_control::ColumnDescContents> = bound_cols
        .iter()
        .map(|(desc_idx, _)| &all_col_descs[*desc_idx])
        .collect();

    let ism_columns = read_ism_file(data_path, dm_blob, &col_descs, nrrow)?;

    for (col_name, raw_data) in &ism_columns {
        let col_desc = col_descs
            .iter()
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "ISM returned column '{col_name}' not in descriptor"
                ))
            })?;

        for (row_idx, row) in rows.iter_mut().enumerate() {
            let value = extract_row_value(raw_data, col_desc, row_idx, nrrow)?;
            row.push(RecordField::new(col_name.clone(), value));
        }
    }

    Ok(())
}

/// Create a Value representing an undefined variable-shape array cell.
///
/// For ndim > 0, returns a zero-sized array with `ndim` dimensions (all sizes 0).
/// For ndim == 0, returns a 0-D empty array.
fn make_undefined_array(dt: CasacoreDataType, ndim: usize) -> casacore_types::Value {
    use casacore_types::{ArrayValue, Value};
    use ndarray::{ArrayD, IxDyn};

    let shape = vec![0usize; ndim.max(1)];
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
        CasacoreDataType::TpComplex => ArrayValue::Complex32(ArrayD::from_elem(
            s,
            casacore_types::Complex32::new(0.0, 0.0),
        )),
        CasacoreDataType::TpDComplex => ArrayValue::Complex64(ArrayD::from_elem(
            s,
            casacore_types::Complex64::new(0.0, 0.0),
        )),
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
