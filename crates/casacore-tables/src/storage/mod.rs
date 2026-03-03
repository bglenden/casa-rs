// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

pub(crate) mod canonical;
pub(crate) mod data_type;
pub(crate) mod incremental_stman;
pub(crate) mod standard_stman;
pub(crate) mod stman_aipsio;
pub(crate) mod table_control;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use casacore_aipsio::ByteOrder;
use casacore_types::{RecordField, RecordValue};
use thiserror::Error;

use crate::schema::{SchemaError, TableSchema};

use self::incremental_stman::{read_ism_file, write_ism_file};
use self::standard_stman::{read_ssm_file, write_ssm_file};
use self::stman_aipsio::{StManColumnInfo, extract_row_value, read_stman_file, write_stman_file};
pub(crate) use self::table_control::RefTableDatContents;

use self::table_control::{
    TableDatContents, TableDatResult, read_table_dat_dispatch, write_concat_table_dat,
    write_ref_table_dat, write_table_dat,
};

pub(crate) const TABLE_CONTROL_FILE: &str = "table.dat";
pub(crate) const TABLE_DATA_FILE_PREFIX: &str = "table.f";
pub(crate) const TABLE_INFO_FILE: &str = "table.info";

#[derive(Debug, Error)]
pub(crate) enum StorageError {
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StorageSnapshot {
    pub(crate) rows: Vec<RecordValue>,
    pub(crate) keywords: RecordValue,
    pub(crate) column_keywords: HashMap<String, RecordValue>,
    pub(crate) schema: Option<TableSchema>,
}

pub(crate) trait StorageManager {
    fn load(&self, table_path: &Path) -> Result<StorageSnapshot, StorageError>;
    fn save(
        &self,
        table_path: &Path,
        snapshot: &StorageSnapshot,
        dm_kind: crate::table::DataManagerKind,
        big_endian: bool,
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
    ) -> Result<(), StorageError> {
        use crate::table::DataManagerKind;

        fs::create_dir_all(table_path)?;

        let schema = snapshot.schema.as_ref().ok_or_else(|| {
            StorageError::FormatMismatch("cannot save without schema".to_string())
        })?;

        let nrrow = snapshot.rows.len() as u64;
        let data_path = table_path.join(format!("{}0", TABLE_DATA_FILE_PREFIX));

        let dm_type_name;
        let dm_data;
        match dm_kind {
            DataManagerKind::StManAipsIO => {
                dm_type_name = "StManAipsIO".to_string();
                dm_data = Vec::new();
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
                // StManAipsIO always uses canonical (big-endian) AipsIO.
                write_stman_file(
                    &data_path,
                    &table_dat.table_desc.columns,
                    &snapshot.rows,
                    ByteOrder::BigEndian,
                )?;
            }
            DataManagerKind::StandardStMan => {
                dm_type_name = "StandardStMan".to_string();
                // Write SSM data file first (it returns the DM blob)
                let table_dat_tmp = TableDatContents::from_snapshot(
                    schema,
                    &snapshot.keywords,
                    &snapshot.column_keywords,
                    nrrow,
                    "StandardStMan",
                    &[],
                    big_endian,
                );
                dm_data = write_ssm_file(
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
        }

        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, "Type = \nSubType = \n")?;

        Ok(())
    }
}

impl CompositeStorage {
    /// Load a PlainTable from table.dat contents and data files.
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

        for dm in &table_dat.column_set.data_managers {
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
                other => {
                    return Err(StorageError::UnsupportedDataManager(other.to_string()));
                }
            }
        }

        Ok(StorageSnapshot {
            rows,
            keywords,
            column_keywords,
            schema: Some(schema),
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

        Ok(StorageSnapshot {
            rows,
            keywords: parent.keywords,
            column_keywords: parent.column_keywords,
            schema,
        })
    }

    /// Save a RefTable to disk (table.dat only, no data files).
    pub(crate) fn save_ref_table(
        &self,
        table_path: &Path,
        contents: &RefTableDatContents,
    ) -> Result<(), StorageError> {
        fs::create_dir_all(table_path)?;
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        write_ref_table_dat(&control_path, contents)?;
        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, "Type = \nSubType = \n")?;
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

        Ok(StorageSnapshot {
            rows: all_rows,
            keywords,
            column_keywords,
            schema,
        })
    }

    /// Save a ConcatTable to disk (table.dat + table.info only, no data files).
    pub(crate) fn save_concat_table(
        &self,
        table_path: &Path,
        contents: &table_control::ConcatTableDatContents,
    ) -> Result<(), StorageError> {
        fs::create_dir_all(table_path)?;
        let control_path = table_path.join(TABLE_CONTROL_FILE);
        write_concat_table_dat(&control_path, contents)?;
        let info_path = table_path.join(TABLE_INFO_FILE);
        fs::write(&info_path, "Type = \nSubType = \n")?;
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
        for (row_idx, row) in rows.iter_mut().enumerate() {
            let value =
                extract_row_value(&stman_data.columns[stman_col_idx], col_desc, row_idx, nrrow)?;
            row.push(RecordField::new(col_desc.col_name.clone(), value));
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

    for (col_name, raw_data) in &ssm_columns {
        let col_desc = col_descs
            .iter()
            .find(|c| c.col_name == *col_name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "SSM returned column '{col_name}' not in descriptor"
                ))
            })?;

        for (row_idx, row) in rows.iter_mut().enumerate() {
            let value = extract_row_value(raw_data, col_desc, row_idx, nrrow)?;
            row.push(RecordField::new(col_name.clone(), value));
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
