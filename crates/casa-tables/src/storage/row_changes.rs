// SPDX-License-Identifier: LGPL-3.0-or-later
//! Change row counts in an existing directory without rebinding columns.
//! Stage modified files using their existing manager sequence numbers. A
//! manager that only extends an index, header or data file needs no copy
//! of its existing payload.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use casa_aipsio::ByteOrder;
use casa_types::{RecordField, RecordValue};

use super::table_control::{ColumnDescContents, DataManagerEntry, TableDatContents};
use super::{
    CompositeStorage, StorageError, TABLE_CONTROL_FILE, incremental_stman, standard_stman,
    stman_aipsio, table_control, tiled_stman,
};

mod publish;

pub(crate) enum Rows<'a> {
    Append {
        count: usize,
        defaults: &'a RecordValue,
    },
    AppendRecords {
        records: &'a [RecordValue],
        defaults: &'a RecordValue,
    },
    Keep(&'a [usize]),
}

impl Rows<'_> {
    fn len(&self, old: u64) -> Result<u64, StorageError> {
        match *self {
            Rows::Append { count, .. } => old
                .checked_add(count as u64)
                .ok_or_else(|| StorageError::FormatMismatch("table row count overflow".into())),
            Rows::AppendRecords { records, .. } => old
                .checked_add(records.len() as u64)
                .ok_or_else(|| StorageError::FormatMismatch("table row count overflow".into())),
            Rows::Keep(rows) => Ok(rows.len() as u64),
        }
    }
}

enum Extension {
    Index {
        path: PathBuf,
        offset: u64,
        bytes: Vec<u8>,
    },
    Payload {
        path: PathBuf,
        len: u64,
    },
}

pub(crate) struct StagedEdit<'a> {
    pub(super) table: &'a Path,
    pub(super) staged: tempfile::TempDir,
    extensions: Vec<Extension>,
    preserve_file_identity: bool,
    ssm_retained_end: HashMap<OsString, u64>,
}

impl<'a> StagedEdit<'a> {
    pub(crate) fn stage(
        table: &'a Path,
        mut dat: TableDatContents,
        old_rows: u64,
        rows: Rows<'_>,
    ) -> Result<Self, StorageError> {
        let new_rows = rows.len(old_rows)?;
        let mut edit = Self::new(table)?;
        edit.preserve_file_identity = matches!(rows, Rows::AppendRecords { .. });
        let order = if dat.big_endian {
            ByteOrder::BigEndian
        } else {
            ByteOrder::LittleEndian
        };
        let private_keywords = &dat.table_desc.private_keywords;
        for manager in &mut dat.column_set.data_managers {
            let columns: Vec<_> = dat
                .table_desc
                .columns
                .iter()
                .zip(&dat.column_set.columns)
                .filter(|(_, entry)| entry.dm_seq_nr == manager.seq_nr)
                .map(|(column, _)| column.clone())
                .collect();
            edit.stage_manager(
                manager,
                &columns,
                &rows,
                old_rows,
                new_rows,
                (order, private_keywords),
            )?;
        }
        dat.nrrow = new_rows;
        dat.column_set.nrrow = new_rows;
        edit.stage_control(&dat)?;
        Ok(edit)
    }

    pub(super) fn new(table: &'a Path) -> Result<Self, StorageError> {
        Ok(Self {
            table,
            staged: tempfile::Builder::new()
                .prefix(".casa-rs-edit-")
                .tempdir_in(table)?,
            extensions: Vec::new(),
            preserve_file_identity: false,
            ssm_retained_end: HashMap::new(),
        })
    }

    pub(super) fn stage_control(&self, dat: &TableDatContents) -> Result<(), StorageError> {
        let dat = crate::table_path::stored_table_dat_keywords(dat, self.table);
        fs::write(
            self.staged.path().join(TABLE_CONTROL_FILE),
            table_control::serialize_table_dat(&dat)?,
        )?;
        Ok(())
    }

    fn stage_manager(
        &mut self,
        manager: &mut DataManagerEntry,
        columns: &[ColumnDescContents],
        rows: &Rows<'_>,
        old_rows: u64,
        new_rows: u64,
        layout: (ByteOrder, &RecordValue),
    ) -> Result<(), StorageError> {
        let (order, private_keywords) = layout;
        let name = format!("table.f{}", manager.seq_nr);
        let original = self.table.join(&name);
        let destination = self.staged.path().join(&name);
        let append = matches!(rows, Rows::Append { .. } | Rows::AppendRecords { .. });
        let defined = matches!(rows, Rows::AppendRecords { records, .. } if records.iter().any(|record| {
            columns.iter().any(|column| record.get(&column.col_name).is_some())
        }));
        match manager.type_name.as_str() {
            "IncrementalStMan" if append && !defined && old_rows > 0 => {
                let (offset, bytes) =
                    incremental_stman::appended_ism_index(&original, old_rows, new_rows)?;
                self.extensions.push(Extension::Index {
                    path: original,
                    offset,
                    bytes,
                });
            }
            "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan"
                if append =>
            {
                let fixed = columns
                    .iter()
                    .any(|column| column.is_array && !column.shape.is_empty());
                if defined
                    || (fixed
                        && matches!(
                            manager.type_name.as_str(),
                            "TiledShapeStMan" | "TiledCellStMan"
                        ))
                {
                    let values = self.manager_rows(columns, rows, old_rows, false)?;
                    let tile = tiled_stman::tiled_tile_shape(self.table, manager.seq_nr)?;
                    let name = tiled_stman::append::manager_name(&original)?;
                    tiled_stman::save_tiled_columns(
                        self.staged.path(),
                        manager.seq_nr,
                        &manager.type_name,
                        columns,
                        &values,
                        order == ByteOrder::BigEndian,
                        tile.as_deref(),
                        &name,
                        private_keywords,
                    )?;
                    tiled_stman::append::set_cache(
                        &destination,
                        tiled_stman::append::cache_size(&original)?,
                    )?;
                    return Ok(());
                }
                if let Some((path, len)) = tiled_stman::append::stage_append(
                    self.table,
                    self.staged.path(),
                    manager.seq_nr,
                    old_rows,
                    new_rows,
                    (columns, private_keywords),
                )? {
                    self.extensions.push(Extension::Payload { path, len });
                }
            }
            "IncrementalStMan" if append => {
                let values = self.manager_rows(columns, rows, old_rows, true)?;
                let (endian, size, cache) = incremental_stman::ism_layout(&original)?;
                incremental_stman::write_ism_with_layout(
                    &destination,
                    columns,
                    &values,
                    endian,
                    Some(size),
                    cache,
                )?;
            }
            "StandardStMan" if matches!(rows, Rows::AppendRecords { .. }) => {
                let Rows::AppendRecords { records, defaults } = rows else {
                    unreachable!()
                };
                let retained_end = standard_stman::stage_records(
                    &original,
                    &destination,
                    columns,
                    &manager.data,
                    old_rows,
                    records,
                    defaults,
                )?;
                self.ssm_retained_end.insert(name.into(), retained_end);
            }
            "StandardStMan" | "StManAipsIO" => {
                let values = self.manager_rows(columns, rows, old_rows, false)?;
                if manager.type_name == "StandardStMan" {
                    let name = standard_stman::dm_name_from_blob(&manager.data).unwrap_or_default();
                    manager.data = standard_stman::rewrite_ssm_rows(
                        &original,
                        &destination,
                        columns,
                        &values,
                        &name,
                    )?;
                } else {
                    let name = stman_aipsio::name_from_file(&original).unwrap_or_default();
                    stman_aipsio::write_named_stman_file(
                        &destination,
                        manager.seq_nr,
                        columns,
                        &values,
                        ByteOrder::BigEndian,
                        &name,
                    )?;
                }
            }
            // Virtual columns derive their row count from their sources.
            other if super::is_virtual_engine(other) => {}
            other => return Err(StorageError::UnsupportedDataManager(other.to_string())),
        }
        Ok(())
    }

    fn manager_rows(
        &self,
        columns: &[ColumnDescContents],
        rows: &Rows<'_>,
        old_rows: u64,
        inherit_previous: bool,
    ) -> Result<Vec<RecordValue>, StorageError> {
        let names: Vec<_> = columns
            .iter()
            .map(|column| column.col_name.as_str())
            .collect();
        let mut existing = CompositeStorage
            .load_selected_columns_with_row_hint(self.table, &names, Some(old_rows))?
            .rows;
        let project = |record: &RecordValue, defaults: &RecordValue| {
            RecordValue::new(
                names
                    .iter()
                    .filter_map(|name| {
                        record
                            .get(name)
                            .or_else(|| defaults.get(name))
                            .map(|value| RecordField::new(*name, value.clone()))
                    })
                    .collect(),
            )
        };
        match *rows {
            Rows::Append { count, defaults } => {
                existing.extend(std::iter::repeat_n(project(defaults, defaults), count));
                Ok(existing)
            }
            Rows::AppendRecords { records, defaults } => {
                for record in records {
                    let base = if inherit_previous {
                        existing.last().unwrap_or(defaults)
                    } else {
                        defaults
                    };
                    existing.push(project(record, base));
                }
                Ok(existing)
            }
            Rows::Keep(kept) => {
                let mut original = existing.into_iter();
                let mut next = 0;
                let mut result = Vec::with_capacity(kept.len());
                for &row in kept {
                    result.push(original.nth(row - next).ok_or_else(|| {
                        StorageError::FormatMismatch(format!("missing row {row} during remove"))
                    })?);
                    next = row + 1;
                }
                Ok(result)
            }
        }
    }
}
