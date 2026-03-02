#![allow(dead_code)]

pub(crate) mod canonical;
pub(crate) mod data_type;
pub(crate) mod standard_stman;
pub(crate) mod stman_aipsio;
pub(crate) mod table_control;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use casacore_types::{RecordField, RecordValue};
use thiserror::Error;

use crate::schema::{SchemaError, TableSchema};

use self::standard_stman::{read_ssm_file, write_ssm_file};
use self::stman_aipsio::{StManColumnInfo, extract_row_value, read_stman_file, write_stman_file};
use self::table_control::{TableDatContents, read_table_dat, write_table_dat};

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

        let table_dat = read_table_dat(&control_path)?;
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

            // Collect column descriptors bound to this DM
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

    fn save(
        &self,
        table_path: &Path,
        snapshot: &StorageSnapshot,
        dm_kind: crate::table::DataManagerKind,
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
                );
                let control_path = table_path.join(TABLE_CONTROL_FILE);
                write_table_dat(&control_path, &table_dat)?;
                write_stman_file(&data_path, &table_dat.table_desc.columns, &snapshot.rows)?;
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
                );
                dm_data = write_ssm_file(
                    &data_path,
                    &table_dat_tmp.table_desc.columns,
                    &snapshot.rows,
                )?;
                // Re-create table_dat with the actual DM blob
                let table_dat = TableDatContents::from_snapshot(
                    schema,
                    &snapshot.keywords,
                    &snapshot.column_keywords,
                    nrrow,
                    &dm_type_name,
                    &dm_data,
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

/// Load columns from a StManAipsIO data file into row records.
fn load_stman_aipsio_columns(
    data_path: &Path,
    all_col_descs: &[table_control::ColumnDescContents],
    bound_cols: &[(usize, &table_control::PlainColumnEntry)],
    rows: &mut [RecordValue],
    nrrow: usize,
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

    let stman_data = read_stman_file(data_path, &col_info)?;

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
