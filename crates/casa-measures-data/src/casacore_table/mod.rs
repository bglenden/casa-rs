// SPDX-License-Identifier: LGPL-3.0-or-later
//! Minimal read-only casacore table support for measures runtime data.
//!
//! This module intentionally supports only the subset needed by the CASA
//! measures tables used by `casa-rs`:
//!
//! - plain tables
//! - `IncrementalStMan`
//! - scalar `double` and `String` columns
//! - fixed-shape `double` array columns

mod aipsio_buf;
mod data_type;
mod incremental_stman;
mod stman_array_file;
mod table_control;

use std::collections::HashMap;
use std::path::Path;

use thiserror::Error;

use self::incremental_stman::read_incremental_stman_file;
use self::table_control::{ColumnDesc, read_plain_table_dat};

/// Errors raised while reading a minimal casacore table.
#[derive(Debug, Error)]
pub enum TableReadError {
    #[error("table read i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("table read aipsio error: {0}")]
    AipsIo(String),
    #[error("table format mismatch: {0}")]
    Format(String),
    #[error("unsupported data manager: {0}")]
    UnsupportedDataManager(String),
    #[error("unsupported column access: {0}")]
    UnsupportedColumn(String),
}

/// Column data decoded from a minimal casacore table.
#[derive(Debug, Clone)]
pub enum ColumnData {
    Float64(Vec<f64>),
    String(Vec<String>),
    ArrayFloat64 { values: Vec<f64>, shape: Vec<i32> },
}

/// A decoded plain table with a subset of supported column types.
#[derive(Debug, Clone)]
pub struct PlainTable {
    row_count: usize,
    columns: HashMap<String, ColumnData>,
}

impl PlainTable {
    /// Open a plain measures table from disk.
    pub fn open(path: &Path) -> Result<Self, TableReadError> {
        let dat = read_plain_table_dat(&path.join("table.dat"))?;
        let mut columns = HashMap::new();

        for dm in &dat.data_managers {
            if dm.type_name != "IncrementalStMan" {
                return Err(TableReadError::UnsupportedDataManager(dm.type_name.clone()));
            }
            let descs: Vec<&ColumnDesc> = dat
                .columns
                .iter()
                .filter(|desc| desc.dm_seq_nr == dm.seq_nr)
                .collect();
            let file_path = path.join(format!("table.f{}", dm.seq_nr));
            for (name, data) in
                read_incremental_stman_file(&file_path, &dm.data, &descs, dat.row_count)?
            {
                columns.insert(name, data);
            }
        }

        Ok(Self {
            row_count: dat.row_count,
            columns,
        })
    }

    /// Return the row count.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Borrow a scalar `double` column.
    pub fn scalar_f64(&self, name: &str) -> Result<&[f64], TableReadError> {
        match self.columns.get(name) {
            Some(ColumnData::Float64(values)) => Ok(values),
            Some(_) => Err(TableReadError::UnsupportedColumn(format!(
                "column {name:?} is not a scalar f64 column"
            ))),
            None => Err(TableReadError::Format(format!(
                "missing expected column {name:?}"
            ))),
        }
    }

    /// Borrow a scalar `String` column.
    pub fn scalar_string(&self, name: &str) -> Result<&[String], TableReadError> {
        match self.columns.get(name) {
            Some(ColumnData::String(values)) => Ok(values),
            Some(_) => Err(TableReadError::UnsupportedColumn(format!(
                "column {name:?} is not a scalar string column"
            ))),
            None => Err(TableReadError::Format(format!(
                "missing expected column {name:?}"
            ))),
        }
    }

    /// Borrow a fixed-shape `double` array cell as a flat slice.
    pub fn array_f64_cell(&self, name: &str, row: usize) -> Result<&[f64], TableReadError> {
        match self.columns.get(name) {
            Some(ColumnData::ArrayFloat64 { values, shape }) => {
                let nrelem = shape.iter().map(|dim| *dim as usize).product::<usize>();
                let start = row
                    .checked_mul(nrelem)
                    .ok_or_else(|| TableReadError::Format("array offset overflow".to_string()))?;
                let end = start + nrelem;
                values.get(start..end).ok_or_else(|| {
                    TableReadError::Format(format!(
                        "row {row} out of bounds for array column {name:?}"
                    ))
                })
            }
            Some(_) => Err(TableReadError::UnsupportedColumn(format!(
                "column {name:?} is not an array f64 column"
            ))),
            None => Err(TableReadError::Format(format!(
                "missing expected column {name:?}"
            ))),
        }
    }
}
