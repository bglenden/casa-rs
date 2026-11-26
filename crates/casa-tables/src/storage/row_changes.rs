// SPDX-License-Identifier: LGPL-3.0-or-later
//! Change row counts in an existing directory without rebinding columns.
//! Stage modified files using their existing manager sequence numbers. A
//! manager that only extends an index, header or data file needs no copy
//! of its existing payload.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use casa_aipsio::ByteOrder;
use casa_types::{RecordField, RecordValue, ScalarValue, Value};

use super::table_control::{ColumnDescContents, DataManagerEntry, TableDatContents};
use super::{
    CompositeStorage, StorageError, TABLE_CONTROL_FILE, incremental_stman, standard_stman,
    stman_aipsio, table_control, tiled_stman,
};

mod publish;
mod replace_columns;
pub(crate) use publish::ReversibleEdit;
pub(crate) use replace_columns::replace_private_int32_column;

pub(crate) enum Rows<'a> {
    Append {
        count: usize,
        defaults: &'a RecordValue,
    },
    AppendRecords {
        records: &'a [RecordValue],
        defaults: &'a RecordValue,
        encoded: Option<&'a tiled_stman::shape_append::ShapeAppend>,
        existing_scalar: Option<(&'a str, &'a ScalarValue)>,
    },
    Keep(&'a [usize]),
    ReplaceColumns(&'a BTreeMap<String, Vec<Option<Value>>>),
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
            Rows::ReplaceColumns(_) => Ok(old),
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
    StandardAppend {
        path: PathBuf,
        records: standard_stman::StagedRecords,
    },
}

pub(crate) struct StagedEdit<'a> {
    pub(super) table: &'a Path,
    pub(super) staged: tempfile::TempDir,
    extensions: Vec<Extension>,
    preserve_file_identity: bool,
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
        edit.preserve_file_identity =
            matches!(rows, Rows::AppendRecords { .. } | Rows::ReplaceColumns(_));
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
            if let Rows::ReplaceColumns(values) = rows {
                if !columns
                    .iter()
                    .any(|column| values.contains_key(&column.col_name))
                {
                    continue;
                }
                if edit.stage_replaced_scalars(manager, &columns, values, old_rows)? {
                    continue;
                }
            }
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
        let replacing = matches!(rows, Rows::ReplaceColumns(_));
        let defined = matches!(rows, Rows::AppendRecords { records, .. } if records.iter().any(|record| {
            columns.iter().any(|column| record.get(&column.col_name).is_some())
        }));
        let override_values = matches!(rows, Rows::AppendRecords { existing_scalar: Some((name, _)), .. }
            if columns.iter().any(|column| column.col_name == *name));
        if let Rows::AppendRecords {
            encoded: Some(encoded),
            ..
        } = rows
            && encoded.stage(self.staged.path(), manager.seq_nr, defined)?
        {
            return Ok(());
        }
        match manager.type_name.as_str() {
            "IncrementalStMan" if append && !defined && !override_values && old_rows > 0 => {
                let (offset, bytes) =
                    incremental_stman::appended_ism_index(&original, old_rows, new_rows)?;
                self.extensions.push(Extension::Index {
                    path: original,
                    offset,
                    bytes,
                });
            }
            "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan"
                if append || replacing =>
            {
                let fixed = columns
                    .iter()
                    .any(|column| column.is_array && !column.shape.is_empty());
                if replacing
                    || defined
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
            "IncrementalStMan" if append || replacing => {
                if old_rows > 0
                    && !override_values
                    && columns
                        .iter()
                        .all(|column| !column.is_array && !column.is_record())
                    && let Rows::AppendRecords { records, .. } = rows
                    && incremental_stman::stage_scalar_append(
                        &original,
                        &destination,
                        columns,
                        &manager.data,
                        records,
                        old_rows,
                    )?
                {
                    return Ok(());
                }
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
            "StandardStMan" if !override_values && matches!(rows, Rows::AppendRecords { .. }) => {
                let Rows::AppendRecords {
                    records, defaults, ..
                } = rows
                else {
                    unreachable!()
                };
                let records = standard_stman::stage_records(
                    &original,
                    &destination,
                    columns,
                    &manager.data,
                    old_rows,
                    records,
                    defaults,
                )?;
                self.extensions.push(Extension::StandardAppend {
                    path: original,
                    records,
                });
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
            other if !replacing && super::is_virtual_engine(other) => {}
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
            Rows::AppendRecords {
                records,
                defaults,
                existing_scalar,
                ..
            } => {
                if let Some((name, value)) = existing_scalar
                    && names.contains(&name)
                {
                    for record in &mut existing {
                        record.upsert(name, Value::Scalar(value.clone()));
                    }
                }
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
            Rows::ReplaceColumns(values) => {
                for name in names {
                    if let Some(cells) = values.get(name) {
                        for (row, cell) in existing.iter_mut().zip(cells) {
                            match cell {
                                Some(value) => row.upsert(name, value.clone()),
                                None => {
                                    row.remove(name);
                                }
                            }
                        }
                    }
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
